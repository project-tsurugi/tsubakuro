/*
 * Copyright 2019-2023 Project Tsurugi.
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

class server_wire_container
{
    static constexpr std::size_t shm_size = (1<<20);  // 1M bytes (tentative)
    static constexpr std::size_t resultset_wire_size = (1<<16);  // 64K bytes (tentative)
    static constexpr std::size_t request_buffer_size = (1<<10);
    static constexpr std::size_t response_buffer_size = (1<<16);
    static constexpr std::size_t resultset_buffer_size = (1<<17); //  128K bytes NOLINT

public:
    // resultset_wires_container
    class resultset_wires_container {
    public:
        resultset_wires_container(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::string_view name, std::size_t count)
            : managed_shm_ptr_(managed_shm_ptr), rsw_name_(name) {
            managed_shm_ptr_->destroy<shm_resultset_wires>(rsw_name_.c_str());
            shm_resultset_wires_ = managed_shm_ptr_->construct<shm_resultset_wires>(rsw_name_.c_str())(managed_shm_ptr_, count, resultset_buffer_size);
            current_wire_ = shm_resultset_wires_->acquire();
        }

        void write(const char* from, std::size_t length) {
            current_wire_->write(from, length);
        }
        void flush() {
            current_wire_->flush();
        }
        void set_eor() {
            shm_resultset_wires_->set_eor();
        }

    private:
        boost::interprocess::managed_shared_memory* managed_shm_ptr_;
        std::string rsw_name_;
        shm_resultset_wires* shm_resultset_wires_{};
        shm_resultset_wire* current_wire_{};
    };

    class wire_container {
    public:
        wire_container() = default;
        wire_container(unidirectional_message_wire* wire, char* bip_buffer, server_wire_container* envelope)
            : wire_(wire), bip_buffer_(bip_buffer), envelope_(envelope) {
        }
        message_header peep() {
            auto rv = wire_->peep(bip_buffer_, true);
            envelope_->slot(rv.get_idx());
            return rv;
        }
        std::string_view payload() {
            return wire_->payload(bip_buffer_);
        }
        void dispose() {
            return wire_->dispose();
        }
    private:
        unidirectional_message_wire* wire_{};
        char* bip_buffer_{};
        server_wire_container* envelope_;
    };
    class response_wire_container {
    public:
        response_wire_container() = default;
        response_wire_container(unidirectional_response_wire* wire, char* bip_buffer, server_wire_container* envelope)
            : wire_(wire), bip_buffer_(bip_buffer), envelope_(envelope) {};
        void write(const char* from, response_header header) {
            response_header rh(envelope_->slot(), header.get_length(), header.get_type());
            wire_->write(bip_buffer_, from, rh);
        }
    private:
        unidirectional_response_wire* wire_{};
        char* bip_buffer_{};
        server_wire_container* envelope_;
    };

    using resultset_wire = shm_resultset_wire;

    server_wire_container(std::string_view name) : name_(name) {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
        try {
            managed_shared_memory_ =
                std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, name_.c_str(), shm_size);

            auto req_wire = managed_shared_memory_->construct<unidirectional_message_wire>(request_wire_name)(managed_shared_memory_.get(), request_buffer_size);
            auto res_wire = managed_shared_memory_->construct<unidirectional_response_wire>(response_wire_name)(managed_shared_memory_.get(), response_buffer_size);
            status_provider_ = managed_shared_memory_->construct<status_provider>(status_provider_name)(managed_shared_memory_.get(), "dummy_as_it_is_test");

            request_wire_ = wire_container(req_wire, req_wire->get_bip_address(managed_shared_memory_.get()), this);
            response_wire_ = response_wire_container(res_wire, res_wire->get_bip_address(managed_shared_memory_.get()), this);
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            std::abort();  // FIXME
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    server_wire_container(server_wire_container const&) = delete;
    server_wire_container(server_wire_container&&) = delete;
    server_wire_container& operator = (server_wire_container const&) = delete;
    server_wire_container& operator = (server_wire_container&&) = delete;

    ~server_wire_container() {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
    }

    wire_container& get_request_wire() { return request_wire_; }
    response_wire_container& get_response_wire() { return response_wire_; }
    void write(signed char* from, response_header header) {
        response_wire_.write(reinterpret_cast<char*>(from), header);
    }

    resultset_wires_container *create_resultset_wires(std::string_view name) {
        if (!resultset_wires_) {
            resultset_wires_ = std::make_unique<resultset_wires_container>(managed_shared_memory_.get(), name, 8);
        }
        return resultset_wires_.get();
    }

    void slot(response_header::index_type slot) { slot_ = slot; }
    response_header::index_type slot() { return slot_; }
private:
    std::string name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    wire_container request_wire_;
    response_wire_container response_wire_;
    status_provider* status_provider_{};
    std::unique_ptr<resultset_wires_container> resultset_wires_{};
    response_header::index_type slot_{};
};

class connection_container
{
    static constexpr std::size_t request_queue_size = (1<<12);  // 4K bytes (tentative)

public:
    connection_container(std::string_view name) : name_(name) {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
        try {
            managed_shared_memory_ =
                std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, name_.c_str(), request_queue_size);
            managed_shared_memory_->destroy<connection_queue>(connection_queue::name);
            connection_queue_ = managed_shared_memory_->construct<connection_queue>(connection_queue::name)(10, managed_shared_memory_->get_segment_manager());
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

    ~connection_container() {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
    }
    connection_queue& get_connection_queue() {
        return *connection_queue_;
    }
    
private:
    std::string name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    connection_queue* connection_queue_;
};

};  // namespace tateyama::common::wire
