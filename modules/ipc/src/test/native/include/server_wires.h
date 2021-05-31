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

public:
    wire_container(std::string_view name) : name_(name) {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
        try {
            managed_shared_memory_ =
                std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, name_.c_str(), shm_size);
            managed_shared_memory_->destroy<session_wire>(wire_name);
            session_wire_ = managed_shared_memory_->construct<session_wire>(wire_name)(managed_shared_memory_.get());
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

    simple_wire& get_request_wire() { return session_wire_->get_request_wire(); }
    simple_wire& get_response_wire() { return session_wire_->get_response_wire(); }
    
private:
    std::string name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    session_wire* session_wire_{};  // for debug impl.
};

};  // namespace tsubakuro::common
