/*
 * Copyright 2019-2021 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "wire.h"

namespace tsubakuro::common::wire {

class session_wire_container
{
    static constexpr const char* wire_name = "request_response";
    static constexpr std::size_t max_responses_size = 16;
    static constexpr std::size_t metadata_size_boundary = 256;

public:
    class response
    {
        static constexpr std::size_t max_responses_size = 256;

    public:
        response(session_wire_container *envelope, std::size_t idx) : envelope_(envelope), idx_(idx) {}

        /**
         * @brief Copy and move constructers are deleted.
         */
        response(response const&) = delete;
        response(response&&) = delete;
        response& operator = (response const&) = delete;
        response& operator = (response&&) = delete;

        signed char* read() {
            envelope_->read_all();
            if (length_ > 0) {
                return buffer_;
            }
            return nullptr;
        }
        void set_length(std::size_t length) { length_ = length; }
        std::size_t get_length() { return length_; }
        signed char* get_buffer() { return buffer_; }

        void set_inuse() { inuse_ = true; }
        bool is_inuse() { return inuse_; }
        void dispose() {
            inuse_ = false;
            length_ = 0;
        }

    private:
        bool inuse_{};
        signed char message_{};
        session_wire_container *envelope_;
        std::size_t idx_;
        signed char buffer_[ max_responses_size];
        std::size_t length_{};
    };

    class resultset_wire_container {
    public:
        resultset_wire_container(session_wire_container *envelope, std::string_view name) : envelope_(envelope), rsw_name_(name) {
            resultset_wire_ = envelope_->managed_shared_memory_->find<unidirectional_simple_wire>(rsw_name_.c_str()).first;
        }
        std::size_t peep() { return resultset_wire_->peep().get_length(); }
        std::pair<signed char*, std::size_t> recv_meta() {
            std::size_t length = resultset_wire_->peep().get_length();
            if(length < metadata_size_boundary) {
                resultset_wire_->read(buffer, length);
                return std::pair<signed char*, std::size_t>(buffer, length);
            } else {
                annex_ = std::make_unique<signed char[]>(length);
                resultset_wire_->read(annex_.get(), length);
                return std::pair<signed char*, std::size_t>(annex_.get(), length);
            }
        }
        std::pair<signed char*, std::size_t> get_chunk() {
            return resultset_wire_->get_chunk();
        }
        void dispose(std::size_t length) {
            resultset_wire_->dispose(length);
        }
        bool is_eor() {
            return resultset_wire_->is_eor();
        }
        session_wire_container* get_envelope() { return envelope_; }
    private:
        session_wire_container *envelope_;
        std::string rsw_name_;
        unidirectional_simple_wire* resultset_wire_{};
        signed char buffer[metadata_size_boundary];
        std::unique_ptr<signed char[]> annex_;
    };
    
    session_wire_container(std::string_view name) : db_name_(name) {
        try {
            managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, db_name_.c_str());
            bidirectional_message_wire_ = managed_shared_memory_->find<bidirectional_message_wire>(wire_name).first;
            if (bidirectional_message_wire_ == nullptr) {
                std::abort();  // FIXME
            }
            responses.resize(max_responses_size);
            responses.at(0) = std::make_unique<response>(this, 0);  // prepare one entry that always gets used
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            std::abort();  // FIXME
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    session_wire_container(session_wire_container const&) = delete;
    session_wire_container(session_wire_container&&) = delete;
    session_wire_container& operator = (session_wire_container const&) = delete;
    session_wire_container& operator = (session_wire_container&&) = delete;

    response *write(signed char* msg, std::size_t length) {
        response *r;
        std::size_t idx;
        for (idx = 0 ; idx < responses.size() ; idx++) {
            if((r = responses.at(idx).get()) == nullptr) { goto case_need_create; }
            if(!r->is_inuse()) { goto case_exist; }
        }
        return nullptr;

      case_need_create:
        responses.at(idx) = std::make_unique<response>(this, idx);
        r = responses.at(idx).get();
      case_exist:
        r->set_inuse();
        bidirectional_message_wire_->get_request_wire().write(msg, message_header(idx, length));
        return r;
    }

    void read_all() {
        unidirectional_message_wire& wire = bidirectional_message_wire_->get_response_wire();
        while (true) {
            message_header h = wire.peep();
            if (h.get_length() == 0) { break; }
            response& r = *responses.at(h.get_idx());
            r.set_length(h.get_length());
            wire.read(r.get_buffer(), h.get_length());
        }
    }

    resultset_wire_container *create_resultset_wire(std::string_view name_) {
        return new resultset_wire_container(this, name_);
    }
    void dispose_resultset_wire(resultset_wire_container* container) {
        delete container;
    }

private:
    std::string db_name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    bidirectional_message_wire* bidirectional_message_wire_{};
    std::vector<std::unique_ptr<response>> responses{};
};

class connection_container
{
    static constexpr std::size_t request_queue_size = (1<<12);  // 4K bytes (tentative)

public:
    connection_container(std::string_view db_name) : db_name_(db_name) {
        try {
            managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, db_name_.c_str());
            connection_queue_ = managed_shared_memory_->find<connection_queue>(connection_queue::name).first;
            if (connection_queue_ == nullptr) {
                std::abort();  // FIXME
            }
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            std::abort();  // FIXME
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    connection_container(connection_container const&) = delete;
    connection_container(connection_container&&) = delete;
    connection_container& operator = (connection_container const&) = delete;
    connection_container& operator = (connection_container&&) = delete;

    connection_queue& get_connection_queue() {
        return *connection_queue_;
    }
    
private:
    std::string db_name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    connection_queue* connection_queue_;
};

};  // namespace tsubakuro::common::wire
