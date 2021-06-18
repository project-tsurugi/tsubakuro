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

class wire_container
{
    static constexpr std::size_t shm_size = (1<<20);  // 1M bytes (tentative)
    static constexpr std::size_t resultset_wire_size = (1<<16);  // 64K bytes (tentative)
    static constexpr std::size_t request_buffer_size = (1<<10);
    static constexpr std::size_t response_buffer_size = (1<<16);
    
public:
    class resultset_wire_container {
    public:
        resultset_wire_container(wire_container *envelope, std::string_view name) : envelope_(envelope), rsw_name_(name) {
            envelope_->managed_shared_memory_->destroy<unidirectional_simple_wire>(rsw_name_.c_str());
            resultset_wire_ = envelope_->managed_shared_memory_->construct<unidirectional_simple_wire>(rsw_name_.c_str())(envelope_->managed_shared_memory_.get(), resultset_wire_size);
            bip_buffer_ = resultset_wire_->get_bip_address(envelope_->managed_shared_memory_.get());
        }
        unidirectional_simple_wire& get_resultset_wire() { return *resultset_wire_; }
        signed char* get_bip_buffer() { return bip_buffer_; }
    private:
        wire_container *envelope_;
        signed char* bip_buffer_;
        std::string rsw_name_;
        unidirectional_simple_wire* resultset_wire_{};
    };
    
    wire_container(std::string_view name) : name_(name) {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
        try {
            managed_shared_memory_ =
                std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, name_.c_str(), shm_size);
            managed_shared_memory_->destroy<unidirectional_message_wire>(request_wire_name);
            managed_shared_memory_->destroy<unidirectional_message_wire>(response_wire_name);
            request_wire_ = managed_shared_memory_->construct<unidirectional_message_wire>(request_wire_name)(managed_shared_memory_.get(), request_buffer_size);
            response_wire_ = managed_shared_memory_->construct<unidirectional_message_wire>(response_wire_name)(managed_shared_memory_.get(), response_buffer_size);
            bip_request_wire_buffer_ = request_wire_->get_bip_address(managed_shared_memory_.get());
            bip_response_wire_buffer_ = response_wire_->get_bip_address(managed_shared_memory_.get());
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            std::abort();  // FIXME
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    wire_container(wire_container const&) = delete;
    wire_container(wire_container&&) = delete;
    wire_container& operator = (wire_container const&) = delete;
    wire_container& operator = (wire_container&&) = delete;

    ~wire_container() {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
    }

    unidirectional_message_wire& get_request_wire() { return *request_wire_; }
    unidirectional_message_wire& get_response_wire() { return *response_wire_; }
    signed char* get_request_bip_buffer() { return bip_request_wire_buffer_; }
    signed char* get_response_bip_buffer() { return bip_response_wire_buffer_; }

    resultset_wire_container *create_resultset_wire(std::string_view name_) {
        return new resultset_wire_container(this, name_);
    }
    
private:
    std::string name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    unidirectional_message_wire* request_wire_;
    unidirectional_message_wire* response_wire_;
    signed char* bip_request_wire_buffer_;
    signed char* bip_response_wire_buffer_;
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
            connection_queue_ = managed_shared_memory_->construct<connection_queue>(connection_queue::name)();
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

};  // namespace tsubakuro::common
