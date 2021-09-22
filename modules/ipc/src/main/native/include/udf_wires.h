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

namespace tateyama::common::wire {

class session_wire_container
{
    static constexpr std::size_t metadata_size_boundary = 256;

public:
    class resultset_wires_container {
    public:
        resultset_wires_container(session_wire_container *envelope, std::string_view name)
            : envelope_(envelope), managed_shm_ptr_(envelope_->managed_shared_memory_.get()), rsw_name_(name) {
            shm_resultset_wires_ = managed_shm_ptr_->find<shm_resultset_wires>(rsw_name_.c_str()).first;
            if (shm_resultset_wires_ == nullptr) {
                throw std::runtime_error("cannot find a result_set wire with the specified name");
            }
        }
        std::pair<char*, std::size_t> get_chunk() {
            if (current_wire_ == nullptr) {
                current_wire_ = search();
            }
            if (current_wire_ != nullptr) {
                return current_wire_->get_chunk(current_wire_->get_bip_address(managed_shm_ptr_));
            }
            std::abort();  //  FIXME
        }
        void dispose(std::size_t length) {
            if (current_wire_ != nullptr) {
                current_wire_->dispose(length);
                current_wire_ = nullptr;
                return;
            }
            std::abort();  //  FIXME
        }
        bool is_eor() {
            if (current_wire_ == nullptr) {
                current_wire_ = search();
            }
            if (current_wire_ != nullptr) {
                auto rv = current_wire_->is_eor();
                current_wire_ = nullptr;
                return rv;
            }
            std::abort();  //  FIXME
        }
        void set_closed() { shm_resultset_wires_->set_closed(); }
        session_wire_container* get_envelope() { return envelope_; }

    private:
        shm_resultset_wire* search() {
            return shm_resultset_wires_->search();
        }

        session_wire_container *envelope_;
        boost::interprocess::managed_shared_memory* managed_shm_ptr_;
        std::string rsw_name_;
        shm_resultset_wires* shm_resultset_wires_{};
        //   for client
        shm_resultset_wire* current_wire_{};
    };

    class wire_container {
    public:
        wire_container() = default;
        wire_container(unidirectional_message_wire* wire, char* bip_buffer) : wire_(wire), bip_buffer_(bip_buffer) {};
        message_header peep(bool wait = false) {
            return wire_->peep(bip_buffer_, wait);
        }
        void write(const char* from, message_header&& header) {
            wire_->write(bip_buffer_, from, std::move(header));
        }
        void read(char* to, std::size_t msg_len) {
            wire_->read(to, bip_buffer_, msg_len);
        }        
    private:
        unidirectional_message_wire* wire_{};
        char* bip_buffer_{};
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

    response_box::response *write(char* msg, std::size_t length) {
        for (std::size_t idx = 0 ; idx < responses_->size() ; idx++) {
            response_box::response& r = responses_->at(idx);
            if(!r.is_inuse()) {
                r.set_inuse();
                request_wire_.write(msg, message_header(idx, length));
                return &r;
            }
        }
        throw std::runtime_error("the number of pending requests exceeded the number of response boxes");
    }
    resultset_wires_container *create_resultset_wire(std::string_view name_) {
        return new resultset_wires_container(this, name_);
    }
    void dispose_resultset_wire(resultset_wires_container* container) {
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

};  // namespace tateyama::common::wire
