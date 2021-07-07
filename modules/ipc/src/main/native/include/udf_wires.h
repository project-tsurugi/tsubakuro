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
    static constexpr std::size_t metadata_size_boundary = 256;

public:
    class resultset_wire_container {
    public:
        resultset_wire_container(session_wire_container *envelope, std::string_view name) : envelope_(envelope), rsw_name_(name) {
            resultset_wire_ = envelope_->managed_shared_memory_->find<unidirectional_simple_wire>(rsw_name_.c_str()).first;
            if (resultset_wire_ == nullptr) {
                throw std::runtime_error("cannot find a result_set wire with the specified name");
            }
            bip_buffer_ = resultset_wire_->get_bip_address(envelope_->managed_shared_memory_.get());
        }
        std::size_t peep() { return resultset_wire_->peep(bip_buffer_, true).get_length(); }
        std::pair<signed char*, std::size_t> recv_meta() {
            std::size_t length = peep();
            if(length < metadata_size_boundary) {
                resultset_wire_->read(buffer, bip_buffer_, length);
                return std::pair<signed char*, std::size_t>(buffer, length);
            } else {
                annex_ = std::make_unique<signed char[]>(length);
                resultset_wire_->read(annex_.get(), bip_buffer_, length);
                return std::pair<signed char*, std::size_t>(annex_.get(), length);
            }
        }
        std::pair<signed char*, std::size_t> get_chunk() {
            return resultset_wire_->get_chunk(bip_buffer_, !is_eor());
        }
        void dispose(std::size_t length) {
            resultset_wire_->dispose(length);
        }
        bool is_eor() {
            return resultset_wire_->is_eor();
        }
        void set_closed() {
            resultset_wire_->set_closed();
        }
        session_wire_container* get_envelope() { return envelope_; }
    private:
        session_wire_container *envelope_;
        signed char* bip_buffer_;
        std::string rsw_name_;
        unidirectional_simple_wire* resultset_wire_{};
        signed char buffer[metadata_size_boundary];
        std::unique_ptr<signed char[]> annex_;
    };
    
    class wire_container {
    public:
        wire_container() = default;
        wire_container(unidirectional_message_wire* wire, signed char* bip_buffer) : wire_(wire), bip_buffer_(bip_buffer) {};
        message_header peep(bool wait = false) {
            return wire_->peep(bip_buffer_, wait);
        }
        void write(const signed char* from, message_header&& header) {
            wire_->write(bip_buffer_, from, std::move(header));
        }
        void read(signed char* to, std::size_t msg_len) {
            wire_->read(to, bip_buffer_, msg_len);
        }        
    private:
        unidirectional_message_wire* wire_{};
        signed char* bip_buffer_{};
    };

    session_wire_container(std::string_view name) : db_name_(name) {
        try {
            managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, db_name_.c_str());
            auto req_wire = managed_shared_memory_->find<unidirectional_message_wire>(request_wire_name).first;
            responses_ = managed_shared_memory_->find<response_box>(response_box_name).first;
            if (req_wire == nullptr || responses_ == nullptr) {
                throw std::runtime_error("cannot find the session wire");
            }
            request_wire_ = wire_container(req_wire, req_wire->get_bip_address(managed_shared_memory_.get()));
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            throw std::runtime_error("cannot find a session with the specified name");
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    session_wire_container(session_wire_container const&) = delete;
    session_wire_container(session_wire_container&&) = delete;
    session_wire_container& operator = (session_wire_container const&) = delete;
    session_wire_container& operator = (session_wire_container&&) = delete;

    response_box::response *write(signed char* msg, std::size_t length) {
        for (std::size_t idx = 0 ; idx < responses_->size() ; idx++) {
            response_box::response& r = responses_->at(idx);
            if(!r.is_inuse()) {
                r.set_inuse();
                request_wire_.write(msg, message_header(idx, length));
                return &r;
            }
        }
        return nullptr;
    }

    resultset_wire_container *create_resultset_wire(std::string_view name_) {
        return new resultset_wire_container(this, name_);
    }
    void dispose_resultset_wire(resultset_wire_container* container) {
        container->set_closed();
        delete container;
    }

private:
    std::string db_name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    wire_container request_wire_{};
    response_box* responses_;
};

class connection_container
{
    static constexpr std::size_t request_queue_size = (1<<12);  // 4K bytes (tentative)

public:
    connection_container(std::string_view db_name) : db_name_(db_name) {
        try {
            managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, db_name_.c_str());
            connection_queue_ = managed_shared_memory_->find<connection_queue>(connection_queue::name).first;
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            throw std::runtime_error("cannot find a database with the specified name");
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
