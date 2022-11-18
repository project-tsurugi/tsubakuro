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
        std::string_view get_chunk() {
            if (wrap_around_.data()) {
                auto rv = wrap_around_;
                wrap_around_ = std::string_view(nullptr, 0);
                return rv;
            }
            if (current_wire_ == nullptr) {
                current_wire_ = active_wire();
            }
            if (current_wire_ != nullptr) {
                return current_wire_->get_chunk(current_wire_->get_bip_address(managed_shm_ptr_), wrap_around_);
            }
            return std::string_view(nullptr, 0);
        }
        void dispose() {
            if (wrap_around_.data()) {
                return;
            }
            if (current_wire_ != nullptr) {
                current_wire_->dispose(current_wire_->get_bip_address(managed_shm_ptr_));
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
        std::string_view wrap_around_{};
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
        void write(const signed char* from, std::size_t length, bool first = false) {
            if (first) {
                brand_new();
            }
            const char *ptr = reinterpret_cast<const char*>(from);
            for (std::size_t i = 0; i < length; i++) {
                wire_->write(bip_buffer_, *ptr++);
            }
        }
        void flush(message_header::index_type index) {
            wire_->flush(bip_buffer_, index);
        }
        void disconnect() {
            wire_->brand_new();
            wire_->flush(bip_buffer_, message_header::not_use);
        }

    private:
        unidirectional_message_wire* wire_{};
        char* bip_buffer_{};

        void brand_new() {
            wire_->brand_new();
        }
    };

    class response_wire_container {
    public:
        response_wire_container() = default;
        response_wire_container(unidirectional_response_wire* wire, char* bip_buffer) : wire_(wire), bip_buffer_(bip_buffer) {};
        response_header await() {
            return wire_->await(bip_buffer_);
        }
        response_header::length_type get_length() const {
            return wire_->get_length();
        }
        response_header::index_type get_idx() const {
            return wire_->get_idx();
        }
        response_header::msg_type get_type() const {
            return wire_->get_type();
        }
        void read(signed char* to) {
            wire_->read(reinterpret_cast<char*>(to), bip_buffer_);
        }
        void close() {
            wire_->close();
        }

    private:
        unidirectional_response_wire* wire_{};
        char* bip_buffer_{};
    };

    session_wire_container(std::string_view name) : db_name_(name) {
        try {
            managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, db_name_.c_str());
            auto req_wire = managed_shared_memory_->find<unidirectional_message_wire>(request_wire_name).first;
            auto res_wire = managed_shared_memory_->find<unidirectional_response_wire>(response_wire_name).first;
            if (req_wire == nullptr || res_wire == nullptr) {
                throw std::runtime_error("cannot find the session wire");
            }
            request_wire_ = wire_container(req_wire, req_wire->get_bip_address(managed_shared_memory_.get()));
            response_wire_ = response_wire_container(res_wire, res_wire->get_bip_address(managed_shared_memory_.get()));
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

    wire_container& get_request_wire() { return request_wire_; }
    response_wire_container& get_response_wire() { return response_wire_; }

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
    response_wire_container response_wire_{};
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
