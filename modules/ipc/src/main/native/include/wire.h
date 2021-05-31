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

namespace tsubakuro::common::wire {

/**
 * @brief header information used in request/response message,
 * it assumes that machines with the same endianness communicate with each other
 */
class message_header {
public:
    static constexpr std::size_t size = 2 * sizeof(std::size_t);
    
    message_header(std::size_t idx, std::size_t length) : idx_(idx), length_(length) {}
    message_header(signed char* buffer) {
        std::memcpy(&idx_, buffer, sizeof(std::size_t));
        std::memcpy(&length_, buffer + sizeof(std::size_t), sizeof(std::size_t));
    }

    std::size_t get_length() { return length_; }
    std::size_t get_idx() { return idx_; }
    signed char* get_buffer() {
        std::memcpy(buffer_, &idx_, sizeof(std::size_t));
        std::memcpy(buffer_ + sizeof(std::size_t), &length_, sizeof(std::size_t));
        return buffer_;
    };

private:
    std::size_t idx_;
    std::size_t length_;
    signed char buffer_[size];
};

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
        buffer_ = static_cast<signed char*>(managed_shm_ptr_->allocate_aligned(capacity_, Alignment));
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
    void write(signed char* buf, message_header&& header) {
        std::size_t length = header.get_length() + message_header::size;
        while(length > room()) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_full_.wait(lock, [this, length](){ return !(length < room()); } );
        }
        bool was_empty = is_empty();
        std::atomic_thread_fence(std::memory_order_acquire);
        memcpy(write_point(), header.get_buffer(), message_header::size);  // FIXME should care of buffer round up
        memcpy(write_point() + message_header::size, buf, header.get_length());  // FIXME should care of buffer round up
        pushed_ += length;
        if (was_empty) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_empty_.notify_one();
        }
    }

    /**
     * @brief pop the current row.
     */
    message_header peep(bool wait = false) {
        bool was_full = is_full();
        if (wait) {
            while(length() < message_header::size) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                m_empty_.wait(lock, [this](){ return length() >= message_header::size; });
            }
        } else {
            if (length() < message_header::size) { return message_header(0, 0); }
        }
        message_header header(read_point());  // FIXME should care of buffer round up
        poped_ += message_header::size;
        if (was_full) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_full_.notify_one();
        }
        return header;
    }

    /**
     * @brief pop the current row.
     */
    void read(signed char* buf, std::size_t msg_len) {
//        std::atomic_thread_fence(std::memory_order_acquire);
        memcpy(buf, read_point(), msg_len);  // FIXME should care of buffer round up
        poped_ += msg_len;
    }
    std::size_t length() { return (pushed_ - poped_); }

private:
    bool is_empty() const { return pushed_ == poped_; }
    bool is_full() const { return (pushed_ - poped_) >= capacity_; }
    std::size_t room() const { return capacity_ - (pushed_ - poped_); }
    std::size_t index(std::size_t n) const { return n %  capacity_; }
    signed char* read_point() { return buffer_ + index(poped_); }
    signed char* write_point() { return buffer_ + index(pushed_); }
    
    boost::interprocess::managed_shared_memory* managed_shm_ptr_;
    signed char* buffer_;

    std::size_t capacity_;
    std::size_t pushed_{0};
    std::size_t poped_{0};

    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition m_empty_{};
    boost::interprocess::interprocess_condition m_full_{};
};


class session_wire
{
    static constexpr std::size_t request_buffer_size = (1<<10);
    static constexpr std::size_t response_buffer_size = (1<<16);

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

};  // namespace tsubakuro::common
