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

#include <memory>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>

namespace tsubakuro::common {
/**
 * @brief One-to-one unidirectional communication of charactor stream
 */
class simple_wire
{
public:
    /**
     * @brief Construct a new object.
     */
    simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : managed_shm_ptr_(managed_shm_ptr), capacity_(capacity) {
        const std::size_t Alignment = 64;
        buffer_ = static_cast<char *>(managed_shm_ptr_->allocate_aligned(capacity_, Alignment));
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    simple_wire(simple_wire const&) = delete;
    simple_wire(simple_wire&&) = delete;
    simple_wire& operator = (simple_wire const&) = delete;
    simple_wire& operator = (simple_wire&&) = delete;
            
    /**
     * @brief push the writing row into the queue.
     */
    void write(char *buf, std::size_t len) {
        while(len < room()) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_full_.wait(lock, [this, len](){ return !(len < room()); } );
        }
        bool was_empty = is_empty();
        std::atomic_thread_fence(std::memory_order_acquire);
        memcpy(write_point(), buf, len);
        pushed_ += len;
        if (was_empty) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_empty_.notify_one();
        }
    }

    /**
     * @brief pop the current row.
     */
    std::size_t read(char *buf, std::size_t len) {
        while(is_empty()) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_empty_.wait(lock, [this](){ return !is_empty(); } );
        }
        bool was_full = is_full();
        std::atomic_thread_fence(std::memory_order_acquire);
        std::size_t l = (len < length()) ? len : length();
        memcpy(buf, write_point(), l);
        if (was_full) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_full_.notify_one();
        }
        return l;
    }
    std::size_t length() { return (pushed_ - poped_); }

private:
    bool is_empty() const { return pushed_ == poped_; }
    bool is_full() const { return (pushed_ - poped_) >= capacity_; }
    std::size_t room() const { return capacity_ - (pushed_ - poped_); }
    std::size_t index(std::size_t n) const { return n %  capacity_; }
    char *read_point() { return buffer_ + index(poped_); }
    char *write_point() { return buffer_ + index(pushed_); }
    
    boost::interprocess::managed_shared_memory* managed_shm_ptr_;
    char *buffer_;

    std::size_t capacity_;
    std::size_t pushed_{0};
    std::size_t poped_{0};

    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition m_empty_{};
    boost::interprocess::interprocess_condition m_full_{};
};
    
class common_wire : public simple_wire
{
public:
    /**
     * @brief Construct a new object.
     */
    common_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t  capacity) :
        simple_wire(managed_shm_ptr, capacity) {}

    /**
     * @brief Copy and move constructers are deleted.
     */
    common_wire(common_wire const&) = delete;
    common_wire(common_wire&&) = delete;
    common_wire& operator = (common_wire const&) = delete;
    common_wire& operator = (common_wire&&) = delete;

    /**
     * @brief lock this channel. (for server channel)
     */
    void lock() {
        boost::interprocess::scoped_lock lock(m_lock_mutex_);
        while (locked_) {
            m_not_locked_.wait(lock, [this](){ return !locked_; } );
        }
        locked_ = true;
        lock.unlock();
    }

    /**
     * @brief unlock this channel.
     */
    void unlock() {
        boost::interprocess::scoped_lock lock(m_lock_mutex_);
        locked_ = false;
        lock.unlock();
        m_not_locked_.notify_one();
    }

    
private:
    boost::interprocess::interprocess_mutex m_lock_mutex_{};
    boost::interprocess::interprocess_condition m_not_locked_{};
    bool locked_{false};
};


static constexpr std::size_t request_buffer_size = 2^10;
static constexpr std::size_t response_buffer_size = 2^16;

class session_wire
{
public:
    session_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr) :
        request_wire_(managed_shm_ptr, request_buffer_size), response_wire_(managed_shm_ptr, response_buffer_size) {
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    session_wire(session_wire const&) = delete;
    session_wire(session_wire&&) = delete;
    session_wire& operator = (session_wire const&) = delete;
    session_wire& operator = (session_wire&&) = delete;

    simple_wire& get_request_wire() { return request_wire_; }
    simple_wire& get_response_wire() { return response_wire_; }

private:
    simple_wire request_wire_;
    simple_wire response_wire_;
};

static std::size_t shm_size = 2^20;

class session_wire_container
{
public:
    session_wire_container(std::string_view name, bool owner = false) : owner_(owner), name_(name) {
        if (owner_) {
            boost::interprocess::shared_memory_object::remove(name_.c_str());
            try {
                managed_shared_memory_ =
                    std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, name_.c_str(), shm_size);
                managed_shared_memory_->destroy<session_wire>(name_.c_str());
                session_wire_ = managed_shared_memory_->construct<session_wire>(name_.c_str())(managed_shared_memory_.get());
            }
            catch(const boost::interprocess::interprocess_exception& ex) {
                std::abort();  // FIXME
            }
        } else {
            try {
                managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, name_.c_str());
                session_wire_ = managed_shared_memory_->find<session_wire>(name_.c_str()).first;
                if (session_wire_ == nullptr) {
                    std::abort();  // FIXME
                }
            }
            catch(const boost::interprocess::interprocess_exception& ex) {
                std::abort();  // FIXME
            }
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    session_wire_container(session_wire_container const&) = delete;
    session_wire_container(session_wire_container&&) = delete;
    session_wire_container& operator = (session_wire_container const&) = delete;
    session_wire_container& operator = (session_wire_container&&) = delete;

    ~session_wire_container() {
        if (owner_) {
            boost::interprocess::shared_memory_object::remove(name_.c_str());
        }
    }
    simple_wire& get_request_wire() { return session_wire_->get_request_wire(); }
    simple_wire& get_response_wire() { return session_wire_->get_response_wire(); }
    
private:
    bool owner_;
    std::string name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    session_wire* session_wire_{};

    session_wire* get_session_wire() { return session_wire_; };
};


};  // namespace tsubakuro::common
