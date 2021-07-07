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
#include <exception>
#include <stdexcept> // std::runtime_error
#include <ostream>
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
    using length_type = std::uint16_t;
    using index_type = std::uint16_t;

    static constexpr std::size_t size = sizeof(length_type) + sizeof(index_type);
    
    message_header(index_type idx, length_type length) : idx_(idx), length_(length) {}
    message_header() : message_header(0, 0) {}
    explicit message_header(const signed char* buffer) {
        std::memcpy(&idx_, buffer, sizeof(index_type));
        std::memcpy(&length_, buffer + sizeof(index_type), sizeof(length_type));
    }

    length_type get_length() const { return length_; }
    index_type get_idx() const { return idx_; }
    signed char* get_buffer() {
        std::memcpy(buffer_, &idx_, sizeof(index_type));
        std::memcpy(buffer_ + sizeof(index_type), &length_, sizeof(length_type));
        return buffer_;
    };

private:
    index_type idx_;
    length_type length_;
    signed char buffer_[size];
};

/**
 * @brief header information used in metadata message,
 * it assumes that machines with the same endianness communicate with each other
 */
class length_header {
public:
    using length_type = std::uint16_t;

    static constexpr std::size_t size = sizeof(length_type);
    
    explicit length_header(length_type length) : length_(length) {}
    explicit length_header(std::size_t length) : length_(static_cast<length_type>(length)) {}
    length_header() : length_header(static_cast<length_type>(0)) {}
    explicit length_header(const signed char* buffer) {
        std::memcpy(&length_, buffer, sizeof(length_type));
    }

    length_type get_length() const { return length_; }
    signed char* get_buffer() {
        std::memcpy(buffer_, &length_, sizeof(length_type));
        return buffer_;
    };

private:
    length_type length_;
    signed char buffer_[size];
};


static constexpr const char* request_wire_name = "request_wire";
static constexpr const char* response_box_name = "response_box";

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
    simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : capacity_(capacity) {
        const std::size_t Alignment = 64;
        auto buffer = static_cast<signed char*>(managed_shm_ptr->allocate_aligned(capacity_, Alignment));
        buffer_handle_ = managed_shm_ptr->get_handle_from_address(buffer);
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
    void write(signed char* base, const signed char* from, T&& header) {
        std::size_t length = header.get_length() + T::size;
        while(length > room()) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.wait(lock, [this, length](){ return !(length < room()); } );
        }
        bool was_empty = is_empty();
        write_to_buffer(base, point(base, pushed_), header.get_buffer(), T::size);
        write_to_buffer(base, point(base, pushed_ + T::size), from, header.get_length());
        pushed_ += length;
        std::atomic_thread_fence(std::memory_order_release);
        if (was_empty) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }

    /**
     * @brief push record into the queue.
     */
    void write(signed char* base, const signed char* from, std::size_t length) {
        while(length > room()) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.wait(lock, [this, length](){ return !(length < room()); } );
        }
        bool was_empty = is_empty();
        write_to_buffer(base, point(base, pushed_), from, length);
        pushed_ += length;
        std::atomic_thread_fence(std::memory_order_release);
        if (was_empty) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }

    /**
     * @brief poop the current header.
     */
    T peep(const signed char* base, bool wait_flag = false) {
        if (wait_flag) {
            wait(T::size);
        } else {
            if (length() < T::size) { return T(); }
        }
        if ((base + capacity_) >= (read_point(base) + sizeof(T))) {
            T header(read_point(base));  // normal case
            return header;
        }
        signed char buf[sizeof(T)];  // in case for ring buffer full
        std::size_t first_part = capacity_ - index(poped_);
        memcpy(buf, read_point(base), first_part);
        memcpy(buf + first_part, base, sizeof(T) - first_part);
        T header(buf);
        return header;
    }

    /**
     * @brief pop the current message.
     */
    void read(signed char* to, const signed char* base, std::size_t msg_len) {
        bool was_full = is_full();
        read_from_buffer(to, base, read_point(base, T::size), msg_len);
        poped_ += T::size + msg_len;
        std::atomic_thread_fence(std::memory_order_release);
        if (was_full) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.notify_one();
        }
    }

    std::size_t length() { return (pushed_ - poped_); }

    signed char* get_bip_address(boost::interprocess::managed_shared_memory* managed_shm_ptr) {
        return static_cast<signed char*>(managed_shm_ptr->get_address_from_handle(buffer_handle_));
    }

    /**
     * @brief provide the current chunk to MsgPack.
     */
    std::pair<signed char*, std::size_t> get_chunk(signed char* base, bool wait_flag = false) {
        if (chunk_end_ < poped_) {
            chunk_end_ = poped_;
        }
        if (wait_flag) {
            while(chunk_end_ >= pushed_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_empty_.wait(lock, [this](){ return chunk_end_ < pushed_; });
            }
        }
        auto chunk_start = chunk_end_;
        if ((pushed_ / capacity_) == (chunk_start / capacity_)) {
            chunk_end_ = pushed_;
        } else {
            chunk_end_ = (pushed_ / capacity_) * capacity_;
        }
        return std::pair<signed char*, std::size_t>(point(base, chunk_start), chunk_end_ - chunk_start);
    }
    /**
     * @brief dispose of data that has completed read and is no longer needed
     */
    void dispose(std::size_t length) {
        poped_ += length;
        chunk_end_ = 0;
    }

protected:
    bool is_empty() const { return (pushed_ == poped_) || (pushed_ == chunk_end_); }
    bool is_full() const { return (pushed_ - poped_) >= capacity_; }
    std::size_t room() const { return capacity_ - (pushed_ - poped_); }
    std::size_t index(std::size_t n) const { return n %  capacity_; }
    const signed char* read_point(const signed char* buffer) { return buffer + index(poped_); }
    const signed char* read_point(const signed char* buffer, std::size_t offset) { return buffer + index(poped_ + offset); }
    signed char* point(signed char* buffer, std::size_t i) { return buffer + index(i); }
    void wait(std::size_t size) {
        while(length() < size) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.wait(lock, [this, size](){ return length() >= size; });
        }
    }
    void write_to_buffer(signed char *base, signed char* to, const signed char* from, std::size_t length) {
        if((base + capacity_) >= (to + length)) {
            memcpy(to, from, length);
        } else {
            std::size_t first_part = capacity_ - (to - base);
            memcpy(to, from, first_part);
            memcpy(base, from + first_part, length - first_part);
        }
    }
    void read_from_buffer(signed char* to, const signed char *base, const signed char* from, std::size_t length) {
        if((base + capacity_) >= (from + length)) {
            memcpy(to, from, length);
        } else {
            std::size_t first_part = capacity_ - (from - base);
            memcpy(to, from, first_part);
            memcpy(to + first_part, base, length - first_part);
        }
    }

    boost::interprocess::managed_shared_memory::handle_t buffer_handle_{};
    std::size_t capacity_;

    std::size_t pushed_{0};
    std::size_t poped_{0};
    std::size_t chunk_end_{0};

    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition c_empty_{};
    boost::interprocess::interprocess_condition c_full_{};
};


class response_box {
public:
    class response : public std::streambuf {
    public:
        static constexpr std::size_t max_response_message_length = 256;

        response() = default;
        void initilize(std::size_t idx) {
        idx_ = idx;
            dispose();
        }

        /**
         * @brief Copy and move constructers are deleted.
         */
        response(response const&) = delete;
        response(response&&) = delete;
        response& operator = (response const&) = delete;
        response& operator = (response&&) = delete;

        void flush() {
            do {
                boost::interprocess::scoped_lock lock(m_mutex_);
                length_ =  pptr() - pbase();
            } while(false);
            c_empty_.notify_one();
        }
        std::pair<signed char*, std::size_t> recv() {
            return std::pair<signed char*, std::size_t>(reinterpret_cast<signed char*>(buffer_), length_);
        }
        void set_inuse() { inuse_ = true; }
        bool is_inuse() { return inuse_; }
        void dispose() {
            setp(reinterpret_cast<char*>(buffer_), reinterpret_cast<char*>(buffer_ + max_response_message_length));
            length_ = 0;
            inuse_ = false;
        }
        void wait() {
            boost::interprocess::scoped_lock lock(m_mutex_);
            while(length_ == 0) {
                c_empty_.wait(lock, [this](){ return length_ > 0; } );
            }
        }
        void write(const char* from,  std::size_t length) {
            memcpy(buffer_, from, length);
            do {
                boost::interprocess::scoped_lock lock(m_mutex_);
                length_ = length;
            } while(false);
            c_empty_.notify_one();
        }

    private:
        std::size_t idx_;
        std::size_t length_{};
        signed char buffer_[max_response_message_length];
        bool inuse_{};

        boost::interprocess::interprocess_mutex m_mutex_{};
        boost::interprocess::interprocess_condition c_empty_{};
    };

public:
    static constexpr std::size_t max_responses_size = 16;
    using response_box_type = std::array<response, max_responses_size>;

    response_box() {
        for( std::size_t i = 0; i < response_box_.size(); i++) {
            response_box_.at(i).initilize(i);
        }
    }
    std::size_t size() { return response_box_.size(); }
    response& at(std::size_t idx) { return response_box_.at(idx); }

private:
    response_box_type response_box_{};
};


class connection_queue
{
public:
    constexpr static const char* name = "connection_queue";

    /**
     * @brief Construct a new object.
     */
    connection_queue() = default;

    /**
     * @brief Copy and move constructers are deleted.
     */
    connection_queue(connection_queue const&) = delete;
    connection_queue(connection_queue&&) = delete;
    connection_queue& operator = (connection_queue const&) = delete;
    connection_queue& operator = (connection_queue&&) = delete;

    std::size_t request() {
        std::size_t rv;

        boost::interprocess::scoped_lock lock(m_mutex_);
        rv = ++requested_;
        c_requested_.notify_one();
        return rv;
    }
    bool check(std::size_t n, bool wait = false) {
        do {
            if (!wait) {
                return accepted_ >= n;
            } else if (accepted_ >= n) {
                return true;
            } else {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_accepted_.wait(lock, [this, n](){ return (accepted_ >= n); });
            }
        } while (true);
    }
    std::size_t listen(bool wait = false) {
        do {
            if (accepted_ < requested_) {
                return accepted_ + 1;
            }
            if (!wait) {
                return 0;
            } else {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_requested_.wait(lock, [this](){ return (accepted_ < requested_); });
            }
        } while (true);
    }
    void accept(std::size_t n) {
        if (n == (accepted_ + 1)) {
            if (n <= requested_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                accepted_ = n;
                c_accepted_.notify_all();
                return;
            } else {
                throw std::runtime_error("Received an session id that was not requested for connection");
            }
        }
        throw std::runtime_error("The session id is not sequential");
    }

private:
    std::size_t requested_{0};
    std::size_t accepted_{0};
    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition c_requested_{};
    boost::interprocess::interprocess_condition c_accepted_{};
};


class unidirectional_message_wire : public simple_wire<message_header> {
public:
    unidirectional_message_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : simple_wire<message_header>(managed_shm_ptr, capacity) {}
};

class unidirectional_simple_wire : public simple_wire<length_header> {
public:
    unidirectional_simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : simple_wire<length_header>(managed_shm_ptr, capacity) {}
    void set_eor() {
        eor_ = true;
        {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }
    bool is_eor() { return eor_; }
    void set_closed() { closed_ = true; }
    bool is_closed() { return closed_; }
    void initialize() {
        pushed_ = poped_ = chunk_end_ = 0;
        eor_ = closed_ = false;
    }
private:
    bool eor_{false};
    bool closed_{false};
};

};  // namespace tsubakuro::common
