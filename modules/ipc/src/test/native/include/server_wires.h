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
    static constexpr const char* wire_name = "request_response";
    static constexpr std::size_t shm_size = (1<<20);  // 1M bytes (tentative)
    static constexpr std::size_t resultset_wire_size = (1<<16);  // 64K bytes (tentative)

public:
    class resultset_wire_container {
    public:
        resultset_wire_container(wire_container *envelope, std::string_view name) : envelope_(envelope), rsw_name_(name) {
            envelope_->managed_shared_memory_->destroy<bidirectional_message_wire>(rsw_name_.c_str());
            resultset_wire_ = envelope_->managed_shared_memory_->construct<unidirectional_simple_wire>(rsw_name_.c_str())(envelope_->managed_shared_memory_.get(), resultset_wire_size);
        }
        unidirectional_simple_wire& get_resultset_wire() { return *resultset_wire_; }
    private:
        wire_container *envelope_;
        std::string rsw_name_;
        unidirectional_simple_wire* resultset_wire_{};
    };
    
    wire_container(std::string_view name) : name_(name) {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
        try {
            managed_shared_memory_ =
                std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, name_.c_str(), shm_size);
            managed_shared_memory_->destroy<bidirectional_message_wire>(wire_name);
            session_wire_ = managed_shared_memory_->construct<bidirectional_message_wire>(wire_name)(managed_shared_memory_.get());
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

    unidirectional_message_wire& get_request_wire() { return session_wire_->get_request_wire(); }
    unidirectional_message_wire& get_response_wire() { return session_wire_->get_response_wire(); }

    resultset_wire_container *create_resultset_wire(std::string_view name_) {
        return new resultset_wire_container(this, name_);
    }
    
private:
    std::string name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    bidirectional_message_wire* session_wire_{};  // for debug impl.
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
