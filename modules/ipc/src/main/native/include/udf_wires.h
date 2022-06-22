/*
 * Copyright 2019-2022 tsurugi project.
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
        resultset_wires_container(session_wire_container *envelope)
            : envelope_(envelope), managed_shm_ptr_(envelope_->managed_shared_memory_.get()) {
        }
        void connect(std::string_view name) {
            rsw_name_ = name;
            shm_resultset_wires_ = managed_shm_ptr_->find<shm_resultset_wires>(rsw_name_.c_str()).first;
            if (shm_resultset_wires_ == nullptr) {
                std::string msg("cannot find a result_set wire with the specified name: ");
                msg += name;
                throw std::runtime_error(msg.c_str());
            }
        }
        std::pair<char*, std::size_t> get_chunk() {
            if (current_wire_ == nullptr) {
                current_wire_ = active_wire();
            }
            if (current_wire_ != nullptr) {
                return current_wire_->get_chunk(current_wire_->get_bip_address(managed_shm_ptr_));
            }
            return std::pair<char*, std::size_t>(nullptr, 0);
        }
        void dispose(std::size_t length) {
            if (current_wire_ != nullptr) {
                current_wire_->dispose(length);
                current_wire_ = nullptr;
                return;
            }
            std::abort();  //  This must not happen.
        }
        bool is_eor() {
            return shm_resultset_wires_->is_eor();
        }
        void set_closed() { shm_resultset_wires_->set_closed(); }
        session_wire_container* get_envelope() { return envelope_; }

    private:
        shm_resultset_wire* active_wire() {
            return shm_resultset_wires_->active_wire();
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
        void brand_new() {
            wire_->brand_new();
        }
        void write(const int b) {
            wire_->write(bip_buffer_, b);
        }
        void flush(message_header::index_type index) {
            wire_->flush(bip_buffer_, index);
        }
        void read(char* to, std::size_t msg_len) {
            wire_->read(to, bip_buffer_, msg_len);
        }
        void disconnect() {
            wire_->brand_new();
            wire_->flush(bip_buffer_, message_header::not_use);
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
            responses_->connect(managed_shared_memory_.get());
            request_wire_ = wire_container(req_wire, req_wire->get_bip_address(managed_shared_memory_.get()));
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            throw std::runtime_error("cannot find a session with the specified name");
        }
    }

    ~session_wire_container() {
        request_wire_.disconnect();
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    session_wire_container(session_wire_container const&) = delete;
    session_wire_container(session_wire_container&&) = delete;
    session_wire_container& operator = (session_wire_container const&) = delete;
    session_wire_container& operator = (session_wire_container&&) = delete;

    response_box::response *get_response_box() {
        for (std::size_t idx = 0 ; idx < responses_->size() ; idx++) {
            response_box::response& r = responses_->at(idx);
            if(!r.is_inuse()) {
                r.set_inuse();
                index_ = idx;
                return &r;
            }
        }
        return nullptr;
    }
    void write(const int b) {
        if (!header_processed_) {
            request_wire_.brand_new();
            header_processed_ = true;
        }
        request_wire_.write(b);
    }
    void flush() {
        request_wire_.flush(index_);
        index_ = -1;
        header_processed_ = false;
    }
    resultset_wires_container *create_resultset_wire() {
        return new resultset_wires_container(this);
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
    message_header::index_type index_{};
    bool header_processed_{};
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
                std::string msg("cannot find a database with the specified name: ");
                msg += db_name;
                throw std::runtime_error(msg.c_str());
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
