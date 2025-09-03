/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <iostream>
#include <memory>
#include <exception>
#include <atomic>
#include <stdexcept> // std::runtime_error
#include <vector>
#include <string>
#include <string_view>
#include <cstdint>
#include <sys/file.h>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/thread/thread_time.hpp>

namespace tateyama::common::wire {

/**
 * @brief header information used in request message,
 * it assumes that machines with the same endianness communicate with each other
 */
class message_header {
public:
    using length_type = std::uint32_t;
    using index_type = std::uint16_t;
    static constexpr index_type terminate_request = 0xffff;

    static constexpr std::size_t size = sizeof(length_type) + sizeof(index_type);

    message_header(index_type idx, length_type length) noexcept : idx_(idx), length_(length) {}
    message_header() noexcept : message_header(0, 0) {}
    explicit message_header(const char* buffer) noexcept {
        std::memcpy(&idx_, buffer, sizeof(index_type));
        std::memcpy(&length_, buffer + sizeof(index_type), sizeof(length_type));  //NOLINT
    }

    [[nodiscard]] length_type get_length() const { return length_; }
    [[nodiscard]] index_type get_idx() const { return idx_; }
    char* get_buffer() noexcept {
        std::memcpy(buffer_, &idx_, sizeof(index_type));  //NOLINT
        std::memcpy(buffer_ + sizeof(index_type), &length_, sizeof(length_type));  //NOLINT
        return static_cast<char*>(buffer_);
    };

private:
    index_type idx_{};
    length_type length_{};
    char buffer_[size]{};  //NOLINT
};

/**
 * @brief header information used in response message,
 * it assumes that machines with the same endianness communicate with each other
 */
class response_header {
public:
    using index_type = message_header::index_type;
    using msg_type = std::uint16_t;
    using length_type = std::uint32_t;

    static constexpr std::size_t size = sizeof(length_type) + sizeof(index_type) + sizeof(msg_type);

    response_header(index_type idx, length_type length, msg_type type) noexcept : idx_(idx), type_(type), length_(length) {}
    response_header() noexcept : response_header(0, 0, 0) {}
    explicit response_header(const char* buffer) noexcept {
        std::memcpy(&idx_, buffer, sizeof(index_type));
        std::memcpy(&type_, buffer + sizeof(index_type), sizeof(msg_type));  //NOLINT
        std::memcpy(&length_, buffer + (sizeof(index_type) + sizeof(msg_type)), sizeof(length_type));  //NOLINT
    }

    [[nodiscard]] index_type get_idx() const { return idx_; }
    [[nodiscard]] length_type get_length() const { return length_; }
    [[nodiscard]] msg_type get_type() const { return type_; }
    char* get_buffer() noexcept {
        std::memcpy(buffer_, &idx_, sizeof(index_type));  //NOLINT
        std::memcpy(buffer_ + sizeof(index_type), &type_, sizeof(msg_type));  //NOLINT
        std::memcpy(buffer_ + (sizeof(index_type) + sizeof(msg_type)), &length_, sizeof(length_type));  //NOLINT
        return static_cast<char*>(buffer_);
    };

private:
    index_type idx_{};
    msg_type type_{};
    length_type length_{};
    char buffer_[size]{};  //NOLINT
};

/**
 * @brief header information used in metadata message,
 * it assumes that machines with the same endianness communicate with each other
 */
class length_header {
public:
    using length_type = std::uint32_t;

    static constexpr std::size_t size = sizeof(length_type);

    explicit length_header(length_type length) noexcept : length_(length) {}
    explicit length_header(std::size_t length) noexcept : length_(static_cast<length_type>(length)) {}
    length_header() noexcept : length_header(static_cast<length_type>(0)) {}
    explicit length_header(const char* buffer) noexcept {
        std::memcpy(&length_, buffer, sizeof(length_type));
    }

    [[nodiscard]] length_type get_length() const { return length_; }
    char* get_buffer() noexcept {
        std::memcpy(buffer_, &length_, sizeof(length_type));  //NOLINT
        return static_cast<char*>(buffer_);
    };

private:
    length_type length_{};
    char buffer_[size]{};  //NOLINT
};


static constexpr const char* request_wire_name = "request_wire";
static constexpr const char* response_wire_name = "response_wire";
static constexpr const char* status_provider_name = "status_provider";

/**
 * @brief One-to-one unidirectional communication of charactor stream with header T
 */
template <typename T>
class simple_wire
{
public:
    static constexpr std::size_t Alignment = 64;

    /**
     * @brief Construct a new object.
     */
    simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : capacity_(capacity) {
        auto buffer = static_cast<char*>(managed_shm_ptr->allocate_aligned(capacity_, Alignment));
        if (!buffer) {
            throw std::runtime_error("cannot allocate shared memory");
        }
        buffer_handle_ = managed_shm_ptr->get_handle_from_address(buffer);
    }

    /**
     * @brief Construct a new object for result_set_wire.
     */
    simple_wire() : capacity_(0) {
    }

    ~simple_wire() = default;

    /**
     * @brief Copy and move constructers are delete.
     */
    simple_wire(simple_wire const&) = delete;
    simple_wire(simple_wire&&) = delete;
    simple_wire& operator = (simple_wire const&) = delete;
    simple_wire& operator = (simple_wire&&) = delete;

    /**
     * @brief provide the view of the first request message in the queue.
     */
    std::string_view payload(const char* base) {
        auto length = static_cast<std::size_t>(header_received_.get_length());
        if (index(poped_.load() + T::size) < index(poped_.load() + T::size + length)) {
            need_dispose_ = T::size + length;
            return std::string_view(read_address(base, T::size), length);
        }
        copy_of_payload_ = std::make_unique<std::string>();
        copy_of_payload_->resize(length);
        auto address =  copy_of_payload_->data();
        read(address, base);
        return {address, length};  // ring buffer wrap around case
    }
    
    /**
     * @brief read and pop the current message.
     */
    void read(char* top, const char* base) {
        auto length = static_cast<std::size_t>(header_received_.get_length());
        auto msg_length = min(length, max_payload_length());
        read_from_buffer(top, base, read_address(base, T::size), msg_length);
        poped_.fetch_add(T::size + msg_length);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_write_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.notify_one();
        }
        length -= msg_length;
        top += msg_length;  // NOLINT
        while (length > 0) {
            msg_length = min(length, capacity_);
            {
                boost::interprocess::scoped_lock lock(m_mutex_);
                wait_for_read_ = true;
                c_empty_.wait(lock, [this, msg_length](){ return stored() >= msg_length; });
                wait_for_read_ = false;
            }
            read_from_buffer(top, base, read_address(base), msg_length);
            poped_.fetch_add(msg_length);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (wait_for_write_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_full_.notify_one();
            }
            length -= msg_length;
            top += msg_length;  // NOLINT
        }
    }

    /**
     * @brief dispose the message in the queue at read_point that has completed read and is no longer needed
     *  used by endpoint IF
     */
    void dispose() {
        if (need_dispose_ > 0) {
            poped_.fetch_add(need_dispose_);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (wait_for_write_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_full_.notify_one();
            }
            need_dispose_ = 0;
        }
        if (copy_of_payload_) {
            copy_of_payload_ = nullptr;
        }
    }

    char* get_bip_address(boost::interprocess::managed_shared_memory* managed_shm_ptr) noexcept {
        if (buffer_handle_ != 0) {
            return static_cast<char*>(managed_shm_ptr->get_address_from_handle(buffer_handle_));
        }
        return nullptr;
    }

    // for ipc_request
    [[nodiscard]] std::size_t read_point() const { return poped_.load(); }

protected:
    [[nodiscard]] std::size_t stored() const { return (pushed_.load() - poped_.load()); }  //NOLINT
    [[nodiscard]] std::size_t room() const { return capacity_ - stored(); }  //NOLINT
    [[nodiscard]] std::size_t index(std::size_t n) const { return n % capacity_; }  //NOLINT
    [[nodiscard]] std::size_t max_payload_length() const { return capacity_ - T::size; }  //NOLINT
    [[nodiscard]] static std::size_t min(std::size_t a, std::size_t b) { return (a > b) ? b : a; }  //NOLINT
    [[nodiscard]] static std::size_t max(std::size_t a, std::size_t b) { return (a > b) ? a : b; }  //NOLINT
    [[nodiscard]] char* buffer_address(char* base, std::size_t n) noexcept { return base + index(n); }  //NOLINT
    [[nodiscard]] const char* read_address(const char* base, std::size_t offset) const { return base + index(poped_.load() + offset); }  //NOLINT
    [[nodiscard]] const char* read_address(const char* base) const { return base + index(poped_.load()); }  //NOLINT

    void write(char* base, const char* from, T header, std::atomic_bool& closed) {
        std::size_t length = header.get_length() + T::size;
        auto msg_length = min(length, capacity_);
        if (msg_length > room() && !closed.load()) { wait_to_write(msg_length, closed); }
        if (closed.load()) { return; }
        write_in_buffer(base, buffer_address(base, pushed_.load()), header.get_buffer(), T::size);
        if (msg_length > T::size) {
            write_in_buffer(base, buffer_address(base, pushed_.load() + T::size), from, msg_length - T::size);
        }
        pushed_.fetch_add(msg_length);
        length -= msg_length;
        from += (msg_length - T::size);  // NOLINT
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
        while (length > 0) {
            msg_length = min(length, capacity_);
            if (msg_length > room() && !closed.load()) { wait_to_write(msg_length, closed); }
            if (closed.load()) { return; }
            write_in_buffer(base, buffer_address(base, pushed_.load()), from, msg_length);
            pushed_.fetch_add(msg_length);
            length -= msg_length;
            from += msg_length;  // NOLINT
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (wait_for_read_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_empty_.notify_one();
            }
        }
    }
    void wait_to_write(std::size_t length, std::atomic_bool& closed) {
        boost::interprocess::scoped_lock lock(m_mutex_);
        wait_for_write_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        c_full_.wait(lock, [this, length, &closed](){ return room() >= length || closed.load(); });
        wait_for_write_ = false;
    }
    void write_in_buffer(char *base, char* top, const char* from, std::size_t length) noexcept {
        if((base + capacity_) >= (top + length)) {  //NOLINT
            memcpy(top, from, length);
        } else {
            std::size_t first_part = capacity_ - (top - base);
            memcpy(top, from, first_part);
            memcpy(base, from + first_part, length - first_part);  //NOLINT
        }
    }
    void read_from_buffer(char* top, const char *base, const char* from, std::size_t length) const {
        if((base + capacity_) >= (from + length)) {  //NOLINT
            memcpy(top, from, length);
        } else {
            std::size_t first_part = capacity_ - (from - base);
            memcpy(top, from, first_part);
            memcpy(top + first_part, base, length - first_part);  //NOLINT
        }
    }

    void copy_header(const char* base) {
        if ((base + capacity_) >= (read_address(base) + T::size)) {  //NOLINT
            header_received_ = T(read_address(base));  // normal case
        } else {
            char buf[T::size];  // in case for ring buffer full  //NOLINT
            std::size_t first_part = capacity_ - index(poped_.load());
            memcpy(buf, read_address(base), first_part);  //NOLINT
            memcpy(buf + first_part, base, T::size - first_part);  //NOLINT
            header_received_ = T(static_cast<char*>(buf));
        }
    }

    boost::interprocess::managed_shared_memory::handle_t buffer_handle_{};  //NOLINT
    std::size_t capacity_;  //NOLINT

    std::atomic_ulong pushed_{0};  //NOLINT
    std::atomic_ulong poped_{0};  //NOLINT

    std::atomic_bool wait_for_write_{};  //NOLINT
    std::atomic_bool wait_for_read_{};  //NOLINT

    boost::interprocess::interprocess_mutex m_mutex_{};  //NOLINT
    boost::interprocess::interprocess_condition c_empty_{};  //NOLINT
    boost::interprocess::interprocess_condition c_full_{};  //NOLINT
    T header_received_{};  // NOLINT

private:
    std::size_t need_dispose_{};
    std::unique_ptr<std::string> copy_of_payload_{};  // in case of ring buffer wrap around
};

static constexpr std::int64_t MAX_TIMEOUT_US = 10L * 365L * 24L * 3600L * 1000L * 1000L;
inline static std::int64_t u_cap(std::int64_t timeout_us) {
    return (timeout_us > MAX_TIMEOUT_US) ? MAX_TIMEOUT_US : timeout_us;
}

// for request
class unidirectional_message_wire : public simple_wire<message_header> {
    constexpr static std::size_t watch_interval = 2;
public:
    unidirectional_message_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : simple_wire<message_header>(managed_shm_ptr, capacity) {}

    /**
     * @brief wait a request message arives and peep the current header.
     * @return the essage_header if request message has been received, for normal reception of request message.
     *  otherwise, dummy request message whose length is 0 and index is message_header::termination_request for termination request
     * @throws std::runtime_error when timeout occures.
     */
    message_header peep(const char* base) {
        while (true) {
            bool termination_requested = termination_requested_.load();
            bool onetime_notification = onetime_notification_.load();
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if(stored() >= message_header::size) {
                copy_header(base);
                return header_received_;
            }
            if (termination_requested) {
                termination_requested_.store(false);
                return {message_header::terminate_request, 0};
            }
            if (onetime_notification) {
                throw std::runtime_error("received shutdown request from outside the communication partner");
            }
            boost::interprocess::scoped_lock lock(m_mutex_);
            wait_for_read_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (!c_empty_.timed_wait(lock,
                                     boost::get_system_time() + boost::posix_time::microseconds(u_cap(watch_interval * 1000 * 1000)),
                                     [this](){ return (stored() >= message_header::size) || termination_requested_.load() || onetime_notification_.load(); })) {
                wait_for_read_ = false;
                throw std::runtime_error("request has not been received within the specified time");
            }
            wait_for_read_ = false;
        }
    }
    /**
     * @brief wake up the worker immediately.
     */
    void notify() {
        onetime_notification_.store(true);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }
    /**
     * @brief close the request wire, used by the server.
     */
    void close() {
        closed_.store(true);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_write_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.notify_one();
        }
    }

    /**
     * @brief write request message
     * @param base the base address of the request wire
     * @param from the request message to be written in the request wire
     * @param header the header of the request message
     */
    void write(char* base, const char* from, message_header header) {
        simple_wire<message_header>::write(base, from, header, closed_);
    }
    /**
     * @brief wake up the worker thread waiting for request arrival, supposed to be used in server termination.
     */
    void terminate() {
        termination_requested_.store(true);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }

private:
    std::atomic_bool termination_requested_{};
    std::atomic_bool onetime_notification_{};
    std::atomic_bool closed_{};
};


// for response
class unidirectional_response_wire : public simple_wire<response_header> {
public:
    constexpr static std::size_t watch_interval = 5;

    unidirectional_response_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : simple_wire<response_header>(managed_shm_ptr, capacity) {}

    /**
     * @brief wait for response arrival and return its header.
     */
    response_header await(const char* base, std::int64_t timeout_us = 0) {
        if (timeout_us == 0) {
            timeout_us = watch_interval * 1000 * 1000;
        }

        while (true) {
            bool closed_shutdown = closed_.load() || shutdown_.load();
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if(stored() >= response_header::size) {
                break;
            }
            if (closed_shutdown) {
                header_received_ = response_header(0, 0, 0);
                return header_received_;
            }
            {
                boost::interprocess::scoped_lock lock(m_mutex_);
                wait_for_read_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);

                if (!c_empty_.timed_wait(lock,
                                         boost::get_system_time() + boost::posix_time::microseconds(u_cap(timeout_us)),
                                         [this](){ return (stored() >= response_header::size) || closed_.load() || shutdown_.load(); })) {
                    wait_for_read_ = false;
                    throw std::runtime_error("response has not been received within the specified time");
                }
                wait_for_read_ = false;
            }
        }

        if ((base + capacity_) >= (read_address(base) + response_header::size)) {  //NOLINT
            header_received_ = response_header(read_address(base));  // normal case
        } else {
            char buf[response_header::size];  // in case for ring buffer full  //NOLINT
            std::size_t first_part = capacity_ - index(poped_.load());
            memcpy(buf, read_address(base), first_part);  //NOLINT
            memcpy(buf + first_part, base, response_header::size - first_part);  //NOLINT
            header_received_ = response_header(static_cast<char*>(buf));
        }
        return header_received_;
    }
    [[nodiscard]] response_header::length_type get_length() const {
        return header_received_.get_length();
    }
    [[nodiscard]] response_header::index_type get_idx() const {
        return header_received_.get_idx();
    }
    [[nodiscard]] response_header::msg_type get_type() const {
        return header_received_.get_type();
    }
    /**
     * @brief close the response wire, used by the client.
     */
    void close() {
        closed_.store(true);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_write_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.notify_one();
        }
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }
    /**
     * @brief check the session has been shut down
     * @return true if the session has been shut down
     */
    [[nodiscard]] bool check_shutdown() const noexcept {
        return shutdown_.load();
    }

    /**
     * @brief write response message
     * @param base the base address of the response wire
     * @param from the response message to be written in the response wire
     * @param header the header of the response message
     */
    void write(char* base, const char* from, response_header header) {
        simple_wire<response_header>::write(base, from, header, closed_);
    }
    /**
     * @brief notify client of the client of the shutdown
     */
    void notify_shutdown() {
        shutdown_.store(true);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }

private:
    std::atomic_bool closed_{};
    std::atomic_bool shutdown_{};
};


// for resultset
class unidirectional_simple_wires {
public:
    constexpr static std::size_t watch_interval = 5;

    class unidirectional_simple_wire : public simple_wire<length_header> {
        friend unidirectional_simple_wires;

    public:
        /**
         * @brief unidirectional_simple_wires::unidirectional_simple_wire constructer is default
         */
        unidirectional_simple_wire() = default;
        ~unidirectional_simple_wire() = default;

        /**
         * @brief Copy and move constructers are deleted.
         */
        unidirectional_simple_wire(unidirectional_simple_wire const&) = delete;
        unidirectional_simple_wire(unidirectional_simple_wire&&) = delete;
        unidirectional_simple_wire& operator = (unidirectional_simple_wire const&) = delete;
        unidirectional_simple_wire& operator = (unidirectional_simple_wire&&) = delete;

        /**
         * @brief push an unit of data into the wire.
         *  used by server
         */
        void write(const char* from, std::size_t length) {
            if (!closed_ && (length > 0)) {
                if (!continued_) {
                    brand_new();
                    continued_ = true;
                }
                write(get_bip_address(managed_shm_ptr_), from, length);
            }
        }
        /**
         * @brief mark the record boundary and notify the clinet of the record arrival.
         *  used by server
         */
        void flush() {
            if (continued_) {
                flush(get_bip_address(managed_shm_ptr_));
            }
        }
        /**
         * @brief check whether data of length can be written.
         *  used by server
         * @return true if the buffer has the room for data
         */
        [[nodiscard]] bool check_room(std::size_t length) noexcept {
            if (continued_) {
                return room() >= length;
            }
            return room() >= (length + length_header::size);
        }
        /**
         * @brief wait data of length can be written.
         *  used by server
         * @return true if the buffer is closed by the client
         */
        [[nodiscard]] bool wait_room(std::size_t length) {
            if (!continued_) {
                length += length_header::size;
            }
            wait_to_resultset_write(length);
            return closed_;
        }

        /**
         * @brief provide the current chunk.
         *  used by clinet
         */
        std::string_view get_chunk(char* base, std::string_view& wrap_around) {
            copy_header(base);
            auto length = header_received_.get_length();

            // If end is on a boundary, it is considered to be on the same page.
            if (((poped_.load() + length_header::size) / capacity_) == ((poped_.load() + length_header::size + length - 1) / capacity_)) {
                return {read_address(base, length_header::size), length};
            }
            auto buffer_end = (pushed_valid_.load() / capacity_) * capacity_;
            std::size_t first_length = buffer_end - (poped_.load() + length_header::size);
            wrap_around = std::string_view(read_address(base, length_header::size + first_length), length - first_length);
            return {read_address(base, length_header::size), first_length};
        }
        /**
         * @brief dispose of data that has completed read and is no longer needed.
         *  used by clinet
         */
        void dispose(char* base) {
            copy_header(base);
            poped_.fetch_add(header_received_.get_length() + length_header::size);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (wait_for_write_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_full_.notify_one();
            }
        }
        /**
         * @brief check this wire has record.
         *  used by clinet
         */
        [[nodiscard]] bool has_record() const { return stored_valid() > 0; }

        void attach_buffer(boost::interprocess::managed_shared_memory::handle_t handle, std::size_t capacity) noexcept {
            buffer_handle_ = handle;
            capacity_ = capacity;
        }
        void detach_buffer() noexcept {
            buffer_handle_ = 0;
            capacity_ = 0;
        }
        bool equal(boost::interprocess::managed_shared_memory::handle_t handle) {
            return handle == buffer_handle_;
        }
        void reset_handle() noexcept {
            buffer_handle_ = 0;
        }
        [[nodiscard]] boost::interprocess::managed_shared_memory::handle_t get_handle() const {
            return buffer_handle_;
        }

    private:
        void brand_new() {
            std::size_t length = length_header::size;
            if (length > room()) {
                wait_to_resultset_write(length);
            }
            pushed_.fetch_add(length);
        }

        void write(char* base, const char* from, std::size_t length) {
            if (length > room()) {
                wait_to_resultset_write(length);
            }
            write_in_buffer(base, buffer_address(base, pushed_.load()), from, length);
            pushed_.fetch_add(length);
        }

        void flush(char* base) {
            length_header header(pushed_.load() - (pushed_valid_.load() + length_header::size));
            write_in_buffer(base, buffer_address(base, pushed_valid_.load()), header.get_buffer(), length_header::size);
            pushed_valid_.store(pushed_.load());
            std::atomic_thread_fence(std::memory_order_acq_rel);
            envelope_->notify_record_arrival();
            continued_ = false;
        }

        void wait_to_resultset_write(std::size_t length) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            wait_for_write_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            c_full_.wait(lock, [this, length](){ return (room() >= length) || closed_; });
            wait_for_write_ = false;
        }

        void set_closed() {
            closed_ = true;
            if (wait_for_write_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_full_.notify_one();
            }
        }

        void set_environments(unidirectional_simple_wires* envelope, boost::interprocess::managed_shared_memory* managed_shm_ptr) noexcept {
            envelope_ = envelope;
            managed_shm_ptr_ = managed_shm_ptr;
        }

        [[nodiscard]] std::size_t stored_valid() const { return (pushed_valid_.load() - poped_.load()); }

        boost::interprocess::managed_shared_memory* managed_shm_ptr_{};  // used by server only
        std::atomic_ulong pushed_valid_{0};                              // used by server only
        std::atomic_bool closed_{};                                      // written by client, read by server
        bool continued_{};                                               // used by server only
        unidirectional_simple_wires* envelope_{};                        // used by server only
    };


    /**
     * @brief unidirectional_simple_wires constructer
     */
    unidirectional_simple_wires(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t count, std::size_t buffer_size)
        : managed_shm_ptr_(managed_shm_ptr), unidirectional_simple_wires_(count, managed_shm_ptr->get_segment_manager()), buffer_size_(buffer_size), reserved_(static_cast<char*>(managed_shm_ptr->allocate_aligned(buffer_size_, Alignment))) {
        for (auto&& wire: unidirectional_simple_wires_) {
            wire.set_environments(this, managed_shm_ptr);
        }
        if (!reserved_) {
            throw std::runtime_error("cannot allocate shared memory");
        }
    }
    ~unidirectional_simple_wires() {
        try {
            if (reserved_ != nullptr) {
                managed_shm_ptr_->deallocate(reserved_);
            }
            for (auto&& wire: unidirectional_simple_wires_) {
                if (!wire.equal(0)) {
                    managed_shm_ptr_->deallocate(wire.get_bip_address(managed_shm_ptr_));
                    wire.detach_buffer();
                }
            }
        } catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
        }
    }

    /**
     * @brief Copy and move constructers are delete.
     */
    unidirectional_simple_wires(unidirectional_simple_wires const&) = delete;
    unidirectional_simple_wires(unidirectional_simple_wires&&) = delete;
    unidirectional_simple_wires& operator = (unidirectional_simple_wires const&) = delete;
    unidirectional_simple_wires& operator = (unidirectional_simple_wires&&) = delete;

    unidirectional_simple_wire* acquire() {
        if (count_using_ == 0) {
            count_using_ = next_index_ = 1;
            unidirectional_simple_wires_.at(0).attach_buffer(managed_shm_ptr_->get_handle_from_address(reserved_), buffer_size_);
            reserved_ = nullptr;
            only_one_buffer_ = true;
            return &unidirectional_simple_wires_.at(0);
        }

        char* buffer{};
        if (reserved_ != nullptr) {
            buffer = reserved_;
            reserved_ = nullptr;
        } else {
            buffer = static_cast<char*>(managed_shm_ptr_->allocate_aligned(buffer_size_, Alignment));
            if (!buffer) {
                throw std::runtime_error("cannot allocate shared memory");
            }
        }
        auto index = search_free_wire();
        unidirectional_simple_wires_.at(index).attach_buffer(managed_shm_ptr_->get_handle_from_address(buffer), buffer_size_);
        only_one_buffer_ = false;
        return &unidirectional_simple_wires_.at(index);
    }
    void release(unidirectional_simple_wire* wire) {
        char* buffer = wire->get_bip_address(managed_shm_ptr_);

        unidirectional_simple_wires_.at(search_wire(wire)).reset_handle();
        if (reserved_ == nullptr) {
            reserved_ = buffer;
        } else {
            managed_shm_ptr_->deallocate(buffer);
        }
        count_using_--;
    }

    /**
     * @brief search a wire that has record sent by the server
     *  used by clinet
     */
    unidirectional_simple_wire* active_wire(std::int64_t timeout_us = 0) {
        if (timeout_us == 0) {
            timeout_us = watch_interval * 1000 * 1000;
        }

        do {
            for (auto&& wire: unidirectional_simple_wires_) {
                if(wire.has_record()) {
                    return &wire;
                }
            }
            {
                boost::interprocess::scoped_lock lock(m_record_);
                wait_for_record_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                unidirectional_simple_wire* active_wire = nullptr;
                if (!c_record_.timed_wait(lock,
                                          boost::get_system_time() + boost::posix_time::microseconds(u_cap(timeout_us)),
                                          [this, &active_wire](){
                                              bool eor = is_eor();
                                              std::atomic_thread_fence(std::memory_order_acq_rel);
                                              for (auto&& wire: unidirectional_simple_wires_) {
                                                  if (wire.has_record()) {
                                                      active_wire = &wire;
                                                      return true;
                                                  }
                                              }
                                              return eor;
                                          })) {
                    wait_for_record_ = false;
                    throw std::runtime_error("record has not been received within the specified time");
                }
                wait_for_record_ = false;
                if (active_wire != nullptr) {
                    return active_wire;
                }
            }
        } while(!is_eor());

        return nullptr;
    }

    /**
     * @brief notify that the client does not read record any more
     *  used by clinet
     */
    void set_closed() {
        for (auto&& wire: unidirectional_simple_wires_) {
            wire.set_closed();
        }
        closed_ = true;
    }
    /**
     * @brief returns that the client has closed the result set wire
     *  used by server
     */
    [[nodiscard]] bool is_closed() const {
        return closed_;
    }
    /**
     * @brief mark the end of the result set by the sql service
     *  used by server
     */
    void set_eor() {
        eor_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        {
            boost::interprocess::scoped_lock lock(m_record_);
            c_record_.notify_one();
        }
    }
    /**
     * @brief returns that the server has marked the end of the result set
     *  used by client
     */
    [[nodiscard]] bool is_eor() const {
        return eor_;
    }

private:
    std::size_t search_free_wire() noexcept {
        if (count_using_ == next_index_) {
            count_using_++;
            return next_index_++;
        }
        for (std::size_t index = 0; index < next_index_; index++) {
            if (unidirectional_simple_wires_.at(index).equal(0)) {
                count_using_++;
                return index;
            }
        }
        std::abort();  // FIXME
    }
    std::size_t search_wire(unidirectional_simple_wire* wire) noexcept {
        for (std::size_t index = 0; index < next_index_; index++) {
            if (unidirectional_simple_wires_.at(index).equal(wire->get_handle())) {
                return index;
            }
        }
        std::abort();  // FIXME
    }

    /**
     * @brief notify the arrival of a record
     *  used by server
     */
    void notify_record_arrival() {
        boost::interprocess::scoped_lock lock(m_record_);
        c_record_.notify_one();
    }

    static constexpr std::size_t Alignment = 64;
    using allocator = boost::interprocess::allocator<unidirectional_simple_wire, boost::interprocess::managed_shared_memory::segment_manager>;

    boost::interprocess::managed_shared_memory* managed_shm_ptr_;  // used by server only
    std::vector<unidirectional_simple_wire, allocator> unidirectional_simple_wires_;
    std::size_t buffer_size_;

    char* reserved_{};
    std::size_t count_using_{};
    std::size_t next_index_{};
    bool only_one_buffer_{};

    std::atomic_bool eor_{};
    std::atomic_bool closed_{};
    std::atomic_bool wait_for_record_{};
    boost::interprocess::interprocess_mutex m_record_{};
    boost::interprocess::interprocess_condition c_record_{};
};

using shm_resultset_wire = unidirectional_simple_wires::unidirectional_simple_wire;
using shm_resultset_wires = unidirectional_simple_wires;


// for status
class status_provider {
    using char_allocator = boost::interprocess::allocator<char, boost::interprocess::managed_shared_memory::segment_manager>;

public:
    status_provider(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::string_view file) : mutex_file_(file, managed_shm_ptr->get_segment_manager()) {
    }

    [[nodiscard]] std::string is_alive() {
        int fd = open(mutex_file_.c_str(), O_RDONLY);  // NOLINT
        if (fd < 0) {
            std::stringstream ss{};
            ss << "cannot open the lock file (" << mutex_file_.c_str() << ")";
            return ss.str();
        }
        if (flock(fd, LOCK_EX | LOCK_NB) == 0) {  // NOLINT
            flock(fd, LOCK_UN);
            close(fd);
            std::stringstream ss{};
            ss << "the lock file (" << mutex_file_.c_str() << ") is not locked, possibly due to server process lost";
            return ss.str();
        }
        close(fd);
        return {};
    }

private:
    boost::interprocess::basic_string<char, std::char_traits<char>, char_allocator> mutex_file_;
};


// implements connect operation
class connection_queue
{
public:
    constexpr static const char* name = "connection_queue";

    class index_queue {
        constexpr static std::size_t watch_interval = 5;
        using long_allocator = boost::interprocess::allocator<std::size_t, boost::interprocess::managed_shared_memory::segment_manager>;

    public:
        index_queue(std::size_t size, boost::interprocess::managed_shared_memory::segment_manager* mgr) : queue_(mgr), capacity_(size) {
            queue_.resize(capacity_);
        }
        void fill(std::uint8_t admin_slots) {
            for (std::size_t i = 0; i < capacity_; i++) {
                queue_.at(i) = i;
            }
            pushed_.store(capacity_ - admin_slots);
        }
        void push(std::size_t sid, std::size_t admin_slots = 0) {
            boost::interprocess::scoped_lock lock(mutex_);
            if (admin_slots > 0 && is_admin(sid)) {
                queue_.at(index(pushed_.load() + admin_slots)) = reset_admin(sid);
                admin_slots_in_use_.fetch_sub(1, std::memory_order_release);
                pushed_.fetch_add(1);
            } else {
                queue_.at(index(pushed_.load() + admin_slots)) = sid;
                pushed_.fetch_add(1, std::memory_order_release);
            }
            std::atomic_thread_fence(std::memory_order_acq_rel);
            condition_.notify_one();
        }
        [[nodiscard]] std::size_t try_pop() {
            boost::interprocess::scoped_lock lock(mutex_);  // trade off
            auto current = poped_.load();
            while (true) {
                auto ps = pushed_.load(std::memory_order_acquire);
                if ((ps + admin_slots_in_use_.load()) <= current) {
                    throw std::runtime_error("no request slot is available for normal request");
                }
                if (poped_.compare_exchange_strong(current, current + 1)) {
                    return queue_.at(index(current));
                }
            }
        }
        [[nodiscard]] std::size_t try_pop(std::uint8_t admin_slots) {
            boost::interprocess::scoped_lock lock(mutex_);
            auto current = poped_.load();
            while (true) {
                auto ps = pushed_.load(std::memory_order_acquire);
                if ((ps + (admin_slots - admin_slots_in_use_.load())) <= current) {
                    throw std::runtime_error("no request slot is available for admin request");
                }
                if (poped_.compare_exchange_strong(current, current + 1)) {
                    admin_slots_in_use_.fetch_add(1);
                    return set_admin(queue_.at(index(current)));
                }
            }
        }
        [[nodiscard]] bool wait(std::atomic_bool& terminate) {
            boost::interprocess::scoped_lock lock(mutex_);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            return condition_.timed_wait(lock,
                                         boost::get_system_time() + boost::posix_time::microseconds(u_cap(watch_interval * 1000 * 1000)),
                                         [this, &terminate](){ return (pushed_.load() > poped_.load()) || terminate.load(); });
        }
        // thread unsafe (assume single listener thread)
        void pop() {
            poped_.fetch_add(1);
        }
        // thread unsafe (assume single listener thread)
        [[nodiscard]] std::size_t front() {
            return queue_.at(index(poped_.load()));
        }
        void notify() {
            condition_.notify_one();
        }

        // for diagnostic
        [[nodiscard]] std::size_t size() const {
            return pushed_.load() - poped_.load();
        }
    private:
        boost::interprocess::vector<std::size_t, long_allocator> queue_;
        std::uint32_t capacity_;
        std::atomic_uint8_t admin_slots_in_use_{0};
        boost::interprocess::interprocess_mutex mutex_{};
        boost::interprocess::interprocess_condition condition_{};

        std::atomic_ulong pushed_{0};
        std::atomic_ulong poped_{0};

        [[nodiscard]] std::size_t index(std::size_t n) const { return n % capacity_; }
    };

    class element {
    public:
        element() = default;
        ~element() = default;

        /**
         * @brief Copy and move constructers.
         */
        element(element const& elm) = delete;
        element(element&& elm) = delete;
        element& operator = (element const&) = delete;
        element& operator = (element&& elm) = delete;

        void accept(std::size_t session_id) {
            session_id_ = session_id;
            notify();
        }
        void reject() {
            session_id_ = session_id_indicating_error;
            notify();
        }
        [[nodiscard]] std::size_t wait(std::int64_t timeout_us = 0) {
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (timeout_us <= 0) {
                boost::interprocess::scoped_lock lock(m_accepted_);
                c_accepted_.wait(lock, [this](){ return (session_id_ != 0); });
            } else {
                boost::interprocess::scoped_lock lock(m_accepted_);
                if (!c_accepted_.timed_wait(lock,
                                            boost::get_system_time() + boost::posix_time::microseconds(u_cap(timeout_us)),
                                            [this](){ return (session_id_ != 0); })) {
                    throw std::runtime_error("connection response has not been accepted within the specified time");
                }
            }
            return session_id_;
        }
        void reuse() {
            session_id_ = 0;
        }
        [[nodiscard]] bool check() const {
            return session_id_ != 0;
        }
    private:
        boost::interprocess::interprocess_mutex m_accepted_{};
        boost::interprocess::interprocess_condition c_accepted_{};
        std::size_t session_id_{};

        void notify() {
            std::atomic_thread_fence(std::memory_order_acq_rel);
            {
                boost::interprocess::scoped_lock lock(m_accepted_);
                c_accepted_.notify_one();
            }
        }
    };

    using element_allocator = boost::interprocess::allocator<element, boost::interprocess::managed_shared_memory::segment_manager>;
    constexpr static std::size_t session_id_indicating_error = UINT64_MAX;
    constexpr static std::size_t admin_bit = 1ULL << 63UL;

    static std::size_t set_admin(std::size_t slot) { return slot | admin_bit; }
    static std::size_t reset_admin(std::size_t slot) { return slot & ~admin_bit; }
    static bool is_admin(std::size_t slot) { return (slot & admin_bit) != 0; }

    /**
     * @brief Construct a new object.
     */
    connection_queue(std::size_t n, boost::interprocess::managed_shared_memory::segment_manager* mgr, std::uint8_t as_n)
        : q_free_(n + as_n, mgr), q_requested_(n + as_n, mgr), v_requested_(n + as_n, mgr), admin_slots_(as_n) {
        q_free_.fill(as_n);
    }
    ~connection_queue() = default;

    /**
     * @brief Copy and move constructers are deleted.
     */
    connection_queue(connection_queue const&) = delete;
    connection_queue(connection_queue&&) = delete;
    connection_queue& operator = (connection_queue const&) = delete;
    connection_queue& operator = (connection_queue&&) = delete;

    std::size_t request() {
        auto sid = q_free_.try_pop();
        q_requested_.push(sid);
        return sid;
    }
    std::size_t request_admin() {
        auto sid = q_free_.try_pop(admin_slots_);
        q_requested_.push(sid);
        return sid;
    }
    std::size_t wait(std::size_t sid, std::int64_t timeout_us = 0) {
        auto& entry = v_requested_.at(reset_admin(sid));
        try {
            auto rtnv = entry.wait(timeout_us);
            entry.reuse();
            return rtnv;
        } catch (std::runtime_error &ex) {
            entry.reuse();
            throw ex;
        }
    }
    bool check(std::size_t sid) {
        return v_requested_.at(reset_admin(sid)).check();
    }
    std::size_t listen() {
        if (q_requested_.wait(terminate_)) {
            return ++session_id_;
        }
        return 0;
    }
    std::size_t slot() {
        return q_requested_.front();
    }
    // either accept() or reject() must be called
    void accept(std::size_t sid, std::size_t session_id) {
        q_requested_.pop();
        v_requested_.at(reset_admin(sid)).accept(session_id);
    }
    // either accept() or reject() must be called
    void reject(std::size_t sid) {
        q_requested_.pop();
        v_requested_.at(reset_admin(sid)).reject();
        q_free_.push(sid, admin_slots_);
    }
    void disconnect(std::size_t sid) {
        q_free_.push(sid, admin_slots_);
    }

    // for terminate
    void request_terminate() {
        terminate_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        q_requested_.notify();
        s_terminated_.wait();
    }
    bool is_terminated() noexcept { return terminate_; }
    void confirm_terminated() { s_terminated_.post(); }

    // for diagnostic
    [[nodiscard]] std::size_t pending_requests() const {
        return q_requested_.size();
    }
    [[nodiscard]] std::size_t session_id_accepted() const {
        return session_id_;
    }

private:
    index_queue q_free_;
    index_queue q_requested_;
    boost::interprocess::vector<element, element_allocator> v_requested_;

    std::atomic_bool terminate_{false};
    std::uint8_t admin_slots_;
    boost::interprocess::interprocess_semaphore s_terminated_{0};

    std::size_t session_id_{};
};

};  // namespace tateyama::common
