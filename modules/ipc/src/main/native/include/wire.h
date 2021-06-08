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
    static constexpr std::size_t size = 2 * sizeof(std::uint16_t);
    
    message_header(std::uint16_t idx, std::uint16_t length) : idx_(idx), length_(length) {}
    message_header() : message_header(0, 0) {}
    explicit message_header(signed char* buffer) {
        std::memcpy(&idx_, buffer, sizeof(std::uint16_t));
        std::memcpy(&length_, buffer + sizeof(std::uint16_t), sizeof(std::uint16_t));
    }

    std::uint16_t get_length() { return length_; }
    std::uint16_t get_idx() { return idx_; }
    signed char* get_buffer() {
        std::memcpy(buffer_, &idx_, sizeof(std::uint16_t));
        std::memcpy(buffer_ + sizeof(std::uint16_t), &length_, sizeof(std::uint16_t));
        return buffer_;
    };

private:
    std::uint16_t idx_;
    std::uint16_t length_;
    signed char buffer_[size];
};

/**
 * @brief header information used in metadata message,
 * it assumes that machines with the same endianness communicate with each other
 */
class length_header {
public:
    static constexpr std::size_t size = sizeof(std::uint16_t);
    
    explicit length_header(std::uint16_t length) : length_(length) {}
    length_header() : length_header(static_cast<std::uint16_t>(0)) {}
    explicit length_header(signed char* buffer) {
        std::memcpy(&length_, buffer, sizeof(std::uint16_t));
    }

    std::uint16_t get_length() { return length_; }
    signed char* get_buffer() {
        std::memcpy(buffer_, &length_, sizeof(std::uint16_t));
        return buffer_;
    };

private:
    std::uint16_t length_;
    signed char buffer_[size];
};


/**
 * @brief One-to-one unidirectional communication of charactor stream with header T
 */
template <typename T>
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
     * @brief push the message into the queue.
     */
    void write(signed char* buf, T&& header) {
        std::size_t length = header.get_length() + T::size;
        while(length > room()) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_full_.wait(lock, [this, length](){ return !(length < room()); } );
        }
        bool was_empty = is_empty();
        memcpy(write_point(), header.get_buffer(), T::size);  // FIXME should care of buffer round up
        memcpy(write_point() + T::size, buf, header.get_length());  // FIXME should care of buffer round up
        pushed_ += length;
        std::atomic_thread_fence(std::memory_order_release);
        if (was_empty) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_empty_.notify_one();
        }
    }

    /**
     * @brief push the writing row into the queue.
     */
    void write(signed char* buf, std::size_t length) {
        while(length > room()) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_full_.wait(lock, [this, length](){ return !(length < room()); } );
        }
        bool was_empty = is_empty();
        memcpy(write_point(), buf, length);  // FIXME should care of buffer round up
        pushed_ += length;
        std::atomic_thread_fence(std::memory_order_release);
        if (was_empty) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_empty_.notify_one();
        }
    }

    /**
     * @brief poop the current header.
     */
    T peep(bool wait = false) {
        if (wait) {
            while(length() < T::size) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                m_empty_.wait(lock, [this](){ return length() >= T::size; });
            }
        } else {
            if (length() < T::size) { return T(); }
        }
        T header(read_point());  // FIXME should care of buffer round up
        return header;
    }

    /**
     * @brief pop the current message.
     */
    void read(signed char* buf, std::size_t msg_len) {
        bool was_full = is_full();
        memcpy(buf, read_point() + T::size, msg_len);  // FIXME should care of buffer round up
        poped_ += T::size + msg_len;
        std::atomic_thread_fence(std::memory_order_release);
        if (was_full) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            m_full_.notify_one();
        }
    }
    std::size_t length() { return (pushed_ - poped_); }

    /**
     * @brief provide the current chunk to MsgPack.
     */
    std::pair<signed char*, std::size_t> get_chunk() {
        if (chunk_end_ != 0) {
            chunk_end_ = 0;            
            return std::pair<signed char*, std::size_t>(point(chunk_end_), pushed_ - chunk_end_);
        }
        if ((pushed_ / capacity_) == (poped_ / capacity_)) {
            return std::pair<signed char*, std::size_t>(read_point(), pushed_ - poped_);
        }
        chunk_end_ = (pushed_ / capacity_) * capacity_;
        return std::pair<signed char*, std::size_t>(read_point(), chunk_end_ - poped_);
    }
    void dispose(std::size_t length) {
        poped_ += length;
    }

private:
    bool is_empty() const { return pushed_ == poped_; }
    bool is_full() const { return (pushed_ - poped_) >= capacity_; }
    std::size_t room() const { return capacity_ - (pushed_ - poped_); }
    std::size_t index(std::size_t n) const { return n %  capacity_; }
    signed char* read_point() { return buffer_ + index(poped_); }
    signed char* write_point() { return buffer_ + index(pushed_); }
    signed char* point(std::size_t i) { return buffer_ + index(i); }
    
    boost::interprocess::managed_shared_memory* managed_shm_ptr_;
    signed char* buffer_;

    std::size_t capacity_;
    std::size_t pushed_{0};
    std::size_t poped_{0};
    std::size_t chunk_end_{0};

    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition m_empty_{};
    boost::interprocess::interprocess_condition m_full_{};
};


class unidirectional_message_wire : public simple_wire<message_header> {
public:
    unidirectional_message_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : simple_wire<message_header>(managed_shm_ptr, capacity) {}
};

class unidirectional_simple_wire : public simple_wire<length_header> {
public:
    unidirectional_simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : simple_wire<length_header>(managed_shm_ptr, capacity) {}
    void set_eor() { eor_ = true; }
    bool is_eor() { return eor_; }
private:
    bool eor_{false};
};


class bidirectional_message_wire
{
    static constexpr std::size_t request_buffer_size = (1<<10);
    static constexpr std::size_t response_buffer_size = (1<<16);

public:
    bidirectional_message_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr) :
        request_wire_(managed_shm_ptr, request_buffer_size), response_wire_(managed_shm_ptr, response_buffer_size) {
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    bidirectional_message_wire(bidirectional_message_wire const&) = delete;
    bidirectional_message_wire(bidirectional_message_wire&&) = delete;
    bidirectional_message_wire& operator = (bidirectional_message_wire const&) = delete;
    bidirectional_message_wire& operator = (bidirectional_message_wire&&) = delete;

    unidirectional_message_wire& get_request_wire() { return request_wire_; }
    unidirectional_message_wire& get_response_wire() { return response_wire_; }

private:
    unidirectional_message_wire request_wire_;
    unidirectional_message_wire response_wire_;
};


class common_wire : public simple_wire<length_header>
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
