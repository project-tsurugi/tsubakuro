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

#include <memory>
#include <exception>
#include <atomic>
#include <stdexcept> // std::runtime_error
#include <vector>
#include <string>
#include <string_view>
#include <sys/file.h>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/thread/thread_time.hpp>

namespace tateyama::common::wire {

/**
 * @brief header information used in request message,
 * it assumes that machines with the same endianness communicate with each other
 */
class message_header {
public:
    using length_type = std::uint16_t;
    using index_type = std::uint16_t;
    static constexpr index_type not_use = 0xffff;

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
    simple_wire() noexcept {
        buffer_handle_ = 0;
        capacity_ = 0;
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
     * @brief peep the current header.
     */
    T peep(const char* base, bool wait_flag = false) {
        while (true) {
            if(stored_valid() >= T::size) {
                break;
            }
            if (wait_flag) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                wait_for_read_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                c_empty_.wait(lock, [this](){ return stored_valid() >= T::size; });
                wait_for_read_ = false;
            } else {
                if (stored_valid() < T::size) { return T(); }
            }
        }
        if ((base + capacity_) >= (read_address(base) + sizeof(T))) {  //NOLINT
            T header(read_address(base));  // normal case
            return header;
        }
        char buf[sizeof(T)];  // in case for ring buffer full  //NOLINT
        std::size_t first_part = capacity_ - index(poped_.load());
        memcpy(buf, read_address(base), first_part);  //NOLINT
        memcpy(buf + first_part, base, sizeof(T) - first_part);  //NOLINT
        T header(static_cast<char*>(buf));
        return header;
    }

    /**
     * @brief begin new record which will be flushed on commit
     */
    void brand_new() {
        std::size_t length = T::size;
        if (length > room()) { wait_to_write(length); }
        pushed_ += length;
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
    std::size_t stored() const { return (pushed_.load() - poped_.load()); }  //NOLINT
    std::size_t room() const { return capacity_ - stored(); }  //NOLINT
    std::size_t index(std::size_t n) const { return n %  capacity_; }  //NOLINT
    char* buffer_address(char* base, std::size_t n) noexcept { return base + index(n); }  //NOLINT
    const char* read_address(const char* base, std::size_t offset) const { return base + index(poped_.load() + offset); }  //NOLINT
    const char* read_address(const char* base) const { return base + index(poped_.load()); }  //NOLINT
    void wait_to_write(std::size_t length) {
        boost::interprocess::scoped_lock lock(m_mutex_);
        wait_for_write_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        c_full_.wait(lock, [this, length](){ return room() >= length; });
        wait_for_write_ = false;
    }
    void write_in_buffer(char *base, char* to, const char* from, std::size_t length) noexcept {
        if((base + capacity_) >= (to + length)) {  //NOLINT
            memcpy(to, from, length);
        } else {
            std::size_t first_part = capacity_ - (to - base);
            memcpy(to, from, first_part);
            memcpy(base, from + first_part, length - first_part);  //NOLINT
        }
    }
    void read_from_buffer(char* to, const char *base, const char* from, std::size_t length) const {
        if((base + capacity_) >= (from + length)) {  //NOLINT
            memcpy(to, from, length);
        } else {
            std::size_t first_part = capacity_ - (from - base);
            memcpy(to, from, first_part);
            memcpy(to + first_part, base, length - first_part);  //NOLINT
        }
    }
    std::size_t stored_valid() const { return (pushed_valid_.load() - poped_.load()); }  //NOLINT

    boost::interprocess::managed_shared_memory::handle_t buffer_handle_{};  //NOLINT
    std::size_t capacity_;  //NOLINT

    std::atomic_ulong pushed_{0};  //NOLINT
    std::atomic_ulong pushed_valid_{0};  //NOLINT
    std::atomic_ulong poped_{0};  //NOLINT

    std::atomic_bool wait_for_write_{};  //NOLINT
    std::atomic_bool wait_for_read_{};  //NOLINT

    boost::interprocess::interprocess_mutex m_mutex_{};  //NOLINT
    boost::interprocess::interprocess_condition c_empty_{};  //NOLINT
    boost::interprocess::interprocess_condition c_full_{};  //NOLINT
};


// for request
class unidirectional_message_wire : public simple_wire<message_header> {
public:
    unidirectional_message_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) noexcept : simple_wire<message_header>(managed_shm_ptr, capacity) {}

    /**
     * @brief push a request message into the queue one by one byte.
     */
    void write(char* base, const int b) {
        std::size_t length = 1;
        char c = b & 0xff;  // NOLINT
        if (length > room()) { wait_to_write(length); }
        write_in_buffer(base, buffer_address(base, pushed_.load()), &c, 1);
        pushed_ += length;
    }

    /**
     * @brief flush the current message.
     */
    void flush(char* base, message_header::index_type index) noexcept {
        message_header header(index, pushed_.load() - (pushed_valid_.load() + message_header::size));
        write_in_buffer(base, buffer_address(base, pushed_valid_.load()), header.get_buffer(), message_header::size);
        pushed_valid_.store(pushed_.load());
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }

    /**
     * @brief get the first request message in the queue.
     */
    std::string_view payload(const char* base) noexcept {
        std::size_t length = peep(base).get_length();
        if (index(poped_.load() + message_header::size) < index(poped_.load() + message_header::size + length)) {
            return std::string_view(read_address(base, message_header::size), length);
        }
        copy_of_payload_ = std::make_unique<std::string>();
        copy_of_payload_->resize(length);
        auto address =  copy_of_payload_->data();
        read_from_buffer(address, base, read_address(base, message_header::size), length);
        return std::string_view(address, length);  // ring buffer wrap around case
    }

    /**
     * @brief pop the current message.
     */
    void read(char* to, const char* base) noexcept {
        std::size_t length = peep(base).get_length();
        read_from_buffer(to, base, read_address(base, message_header::size), length);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_write_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.notify_one();
        }
    }

    /**
     * @brief dispose the message in the queue at read_point that has completed read and is no longer needed
     *  used by endpoint IF
     */
    void dispose(const char* base, const std::size_t read_point) noexcept {
        if (poped_.load() == read_point) {
            poped_ += (peep(base).get_length() + message_header::size);
            if (copy_of_payload_) {
                copy_of_payload_ = nullptr;
            }
            return;
        }
        //  FIXME (Processing when a message located later is discarded first)
    }

private:
    std::unique_ptr<std::string> copy_of_payload_{};  // in case of ring buffer wrap around
};


// for response
class unidirectional_response_wire : public simple_wire<response_header> {
    constexpr static int watch_interval = 5;
public:
    unidirectional_response_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) noexcept : simple_wire<response_header>(managed_shm_ptr, capacity) {}

    /**
     * @brief wait for response arrival and return its header.
     */
    response_header await(const char* base) {
        while (true) {
            if (closed_.load()) {
                header_received_ = response_header(0, 0, 0);
                return header_received_;
            }
            if(stored_valid() >= response_header::size) {
                break;
            }
            {
                boost::interprocess::scoped_lock lock(m_mutex_);
                wait_for_read_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                if (!c_empty_.timed_wait(lock, boost::get_system_time() + boost::posix_time::microseconds(watch_interval * 1000 * 1000), [this](){ return (stored_valid() >= response_header::size) || closed_.load(); })) {
                    wait_for_read_ = false;
                    throw std::runtime_error("response has not been received within the specified time");
                }
                wait_for_read_ = false;
            }
        }

        if ((base + capacity_) >= (read_address(base) + sizeof(response_header))) {  //NOLINT
            header_received_ = response_header(read_address(base));  // normal case
        } else {
            char buf[sizeof(response_header)];  // in case for ring buffer full  //NOLINT
            std::size_t first_part = capacity_ - index(poped_.load());
            memcpy(buf, read_address(base), first_part);  //NOLINT
            memcpy(buf + first_part, base, sizeof(response_header) - first_part);  //NOLINT
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
     * @brief pop the current message.
     */
    void read(char* to, const char* base) noexcept {
        auto length = static_cast<std::size_t>(header_received_.get_length());
        read_from_buffer(to, base, read_address(base, response_header::size), length);
        poped_ += response_header::size + length;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_write_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.notify_one();
        }
    }
    void close() noexcept {
        closed_.store(true);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }

    /**
     * @brief write response message in the response wire, which is used by endpoint IF
     */
    void write(char* base, const char* from, response_header header) {
        std::size_t length = header.get_length() + response_header::size;
        if (length > room()) { wait_to_write(length); }
        write_in_buffer(base, buffer_address(base, pushed_.load() + response_header::size), from, header.get_length());
        pushed_ += length;
        write_in_buffer(base, buffer_address(base, pushed_valid_.load()), header.get_buffer(), response_header::size);
        pushed_valid_.store(pushed_.load());
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }

private:
    response_header header_received_{};
    std::atomic_bool closed_{};

    std::size_t stored_valid() const { return (pushed_valid_.load() - poped_.load()); }  //NOLINT
};


// for resultset
class unidirectional_simple_wires {
public:

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
         *  used by server only
         */
        void write(const char* from, std::size_t length) {
            if (!continued_) {
                brand_new();
                continued_ = true;
            }
            write(get_bip_address(managed_shm_ptr_), from, length);
        }
        void flush() noexcept {
            if (continued_) {
                flush(get_bip_address(managed_shm_ptr_));
            }
        }

        /**
         * @brief provide the current chunk
         */
        std::string_view get_chunk(char* base, std::string_view& wrap_around, std::int64_t timeout = 0) {
            auto header = peep(base, true);
            auto length = header.get_length();

            if (!(stored_valid() >= (length + length_header::size))) {
                if (timeout <= 0) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    wait_for_read_ = true;
                    std::atomic_thread_fence(std::memory_order_acq_rel);
                    c_empty_.wait(lock, [this, length](){ return stored_valid() >= (length + length_header::size); });
                    wait_for_read_ = false;
                } else {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    wait_for_read_ = true;
                    std::atomic_thread_fence(std::memory_order_acq_rel);
                    if (!c_empty_.timed_wait(lock,
#ifdef BOOST_DATE_TIME_HAS_NANOSECONDS
                                             boost::get_system_time() + boost::posix_time::nanoseconds(timeout),
#else
                                             boost::get_system_time() + boost::posix_time::microseconds(((timeout-500)/1000)+1),
#endif
                                             [this, length](){ return stored_valid() >= (length + length_header::size); })) {
                        throw std::runtime_error("record has not been received within the specified time");
                    }
                    wait_for_read_ = false;
                }
            }

            // If end is on a boundary, it is considered to be on the same page.
            if (((poped_.load() + length_header::size) / capacity_) == ((poped_.load() + length_header::size + length - 1) / capacity_)) {
                return std::string_view(read_address(base, length_header::size), length);
            }
            auto buffer_end = (pushed_valid_.load() / capacity_) * capacity_;
            std::size_t first_length = buffer_end - (poped_.load() + length_header::size);
            wrap_around = std::string_view(read_address(base, length_header::size + first_length), length - first_length);
            return std::string_view(read_address(base, length_header::size), first_length);
        }
        /**
         * @brief dispose of data that has completed read and is no longer needed
         */
        void dispose(char* base) {
            auto header = peep(base);
            poped_ += (header.get_length() + length_header::size);
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (wait_for_write_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_full_.notify_one();
            }
        }
        /**
         * @brief check this wire has record
         */
        [[nodiscard]] bool has_record() const { return pushed_valid_.load() > poped_.load(); }

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
        void reset_handle() noexcept { buffer_handle_ = 0; }
        [[nodiscard]] boost::interprocess::managed_shared_memory::handle_t get_handle() const { return buffer_handle_; }

    private:
        void write(char* base, const char* from, std::size_t length) {
            if ((length) > room()) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                wait_for_write_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                c_full_.wait(lock, [this, length](){ return (room() >= length) || closed_; });
                wait_for_write_ = false;
            }
            if (!closed_) {
                write_in_buffer(base, buffer_address(base, pushed_.load()), from, length);
                pushed_ += length;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                if (wait_for_read_) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    c_empty_.notify_one();
                } else {
                    envelope_->notify_record_arrival();
                }
            }
        }

        void flush(char* base) noexcept {
            length_header header(pushed_.load() - (pushed_valid_.load() + length_header::size));
            write_in_buffer(base, buffer_address(base, pushed_valid_.load()), header.get_buffer(), length_header::size);
            pushed_valid_.store(pushed_.load());
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (wait_for_read_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_empty_.notify_one();
            }
            continued_ = false;
        }

        void set_closed() noexcept {
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

        boost::interprocess::managed_shared_memory* managed_shm_ptr_{};  // used by server only
        std::atomic_bool closed_{};  // written by client, read by server
        bool continued_{};  // used by server only
        unidirectional_simple_wires* envelope_{};
    };


    /**
     * @brief unidirectional_simple_wires constructer
     */
    unidirectional_simple_wires(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t count)
        : managed_shm_ptr_(managed_shm_ptr), unidirectional_simple_wires_(count, managed_shm_ptr->get_segment_manager()) {
        for (auto&& wire: unidirectional_simple_wires_) {
            wire.set_environments(this, managed_shm_ptr);
        }
        reserved_ = static_cast<char*>(managed_shm_ptr->allocate_aligned(wire_size, Alignment));
        if (!reserved_) {
            throw std::runtime_error("cannot allocate shared memory");
        }
    }
    ~unidirectional_simple_wires() noexcept {
        if (reserved_ != nullptr) {
            managed_shm_ptr_->deallocate(reserved_);
        }
        for (auto&& wire: unidirectional_simple_wires_) {
            if (!wire.equal(0)) {
                managed_shm_ptr_->deallocate(wire.get_bip_address(managed_shm_ptr_));
                wire.detach_buffer();
            }
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
            unidirectional_simple_wires_.at(0).attach_buffer(managed_shm_ptr_->get_handle_from_address(reserved_), wire_size);
            reserved_ = nullptr;
            only_one_buffer_ = true;
            return &unidirectional_simple_wires_.at(0);
        }

        char* buffer{};
        if (reserved_ != nullptr) {
            buffer = reserved_;
            reserved_ = nullptr;
        } else {
            buffer = static_cast<char*>(managed_shm_ptr_->allocate_aligned(wire_size, Alignment));
            if (!buffer) {
                throw std::runtime_error("cannot allocate shared memory");
            }
        }
        auto index = search_free_wire();
        unidirectional_simple_wires_.at(index).attach_buffer(managed_shm_ptr_->get_handle_from_address(buffer), wire_size);
        only_one_buffer_ = false;
        return &unidirectional_simple_wires_.at(index);
    }
    void release(unidirectional_simple_wire* wire) noexcept {
        char* buffer = wire->get_bip_address(managed_shm_ptr_);

        unidirectional_simple_wires_.at(search_wire(wire)).reset_handle();
        if (reserved_ == nullptr) {
            reserved_ = buffer;
        } else {
            managed_shm_ptr_->deallocate(buffer);
        }
        count_using_--;
    }

    unidirectional_simple_wire* active_wire(std::int64_t timeout = 0) {
        do {
            for (auto&& wire: unidirectional_simple_wires_) {
                if(wire.has_record()) {
                    return &wire;
                }
            }
            if (timeout <= 0) {
                boost::interprocess::scoped_lock lock(m_record_);
                wait_for_record_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                unidirectional_simple_wire* active_wire = nullptr;
                c_record_.wait(lock,
                               [this, &active_wire](){
                                   for (auto&& wire: unidirectional_simple_wires_) {
                                       if (wire.has_record()) {
                                           active_wire = &wire;
                                           return true;
                                       }
                                   }
                                   return is_eor();
                               });
                wait_for_record_ = false;
                if (active_wire != nullptr) {
                    return active_wire;
                }
            } else {
                boost::interprocess::scoped_lock lock(m_record_);
                wait_for_record_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                unidirectional_simple_wire* active_wire = nullptr;
                if (!c_record_.timed_wait(lock,
#ifdef BOOST_DATE_TIME_HAS_NANOSECONDS
                                                boost::get_system_time() + boost::posix_time::nanoseconds(timeout),
#else
                                                boost::get_system_time() + boost::posix_time::microseconds(((timeout-500)/1000)+1),
#endif
                                          [this, &active_wire](){
                                              for (auto&& wire: unidirectional_simple_wires_) {
                                                  if (wire.has_record()) {
                                                      active_wire = &wire;
                                                      return true;
                                                  }
                                              }
                                              return is_eor();
                                          })) {
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
     */
    void set_closed() noexcept {
        for (auto&& wire: unidirectional_simple_wires_) {
            wire.set_closed();
        }
        closed_ = true;
    }
    [[nodiscard]] bool is_closed() const { return closed_; }

    /**
     * @brief mark the end of the result set by the sql service
     */
    void set_eor() noexcept {
        eor_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_record_) {
            boost::interprocess::scoped_lock lock(m_record_);
            c_record_.notify_one();
        }
    }
    [[nodiscard]] bool is_eor() const { return eor_; }

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
     */
    void notify_record_arrival() noexcept {
        if (wait_for_record_) {
            boost::interprocess::scoped_lock lock(m_record_);
            c_record_.notify_one();
        }
    }

    static constexpr std::size_t wire_size = (1<<16);  // 64K bytes (tentative)  //NOLINT
    static constexpr std::size_t Alignment = 64;
    using allocator = boost::interprocess::allocator<unidirectional_simple_wire, boost::interprocess::managed_shared_memory::segment_manager>;

    boost::interprocess::managed_shared_memory* managed_shm_ptr_;  // used by server only
    std::vector<unidirectional_simple_wire, allocator> unidirectional_simple_wires_;

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
    status_provider(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::string_view file) : mutex_file_(managed_shm_ptr->get_segment_manager()) {
        mutex_file_ = file;
    }

    [[nodiscard]] bool is_alive() {
        int fd = open(mutex_file_.c_str(), O_WRONLY);  // NOLINT
        if (fd < 0) {
            return false;
        }
        if (flock(fd, LOCK_EX | LOCK_NB) == 0) {  // NOLINT
            flock(fd, LOCK_UN);
            return false;
        }
        return true;
    }

private:
    boost::interprocess::basic_string<char, std::char_traits<char>, char_allocator> mutex_file_;
};


// implements connect operation
class connection_queue
{
public:
    constexpr static const char* name = "connection_queue";

    /**
     * @brief Construct a new object.
     */
    connection_queue() = default;
    ~connection_queue() = default;

    /**
     * @brief Copy and move constructers are deleted.
     */
    connection_queue(connection_queue const&) = delete;
    connection_queue(connection_queue&&) = delete;
    connection_queue& operator = (connection_queue const&) = delete;
    connection_queue& operator = (connection_queue&&) = delete;

    std::size_t request() noexcept {
        std::size_t rv = requested_.fetch_add(1) + 1;

        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_request_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_requested_.notify_one();
        }
        return rv;
    }
    bool check(std::size_t n, bool wait = false, std::int64_t timeout = 0) {
        if (!wait) {
            return accepted_ >= n;
        }
        if (accepted_ >= n) {
            return true;
        }
        if (timeout <= 0) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            wait_for_accept_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            c_accepted_.wait(lock, [this, n](){ return (accepted_ >= n); });
            wait_for_accept_ = false;
        } else {
            boost::interprocess::scoped_lock lock(m_mutex_);
            wait_for_accept_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (!c_accepted_.timed_wait(lock,
#ifdef BOOST_DATE_TIME_HAS_NANOSECONDS
                                        boost::get_system_time() + boost::posix_time::nanoseconds(timeout),
#else
                                        boost::get_system_time() + boost::posix_time::microseconds(((timeout-500)/1000)+1),
#endif
                                        [this, n](){ return (accepted_ >= n); })) {
                throw std::runtime_error("connection response has not been accepted within the specified time");
            }
            wait_for_accept_ = false;
        }
        return true;
    }
    std::size_t listen(bool wait = false) {
        if (accepted_ < requested_.load() || terminate_) {
            return accepted_ + 1;
        }
        if (!wait) {
            return 0;
        }
        {
            boost::interprocess::scoped_lock lock(m_mutex_);
            wait_for_request_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            c_requested_.wait(lock, [this](){ return (accepted_ < requested_.load()) || terminate_; });
            wait_for_request_ = false;
        }
        return accepted_ + 1;
    }
    void accept(std::size_t n) {
        if (n == (accepted_ + 1)) {
            if (n <= requested_.load()) {
                accepted_ = n;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_accepted_.notify_all();
                return;
            }
            throw std::runtime_error("Received an session id that was not requested for connection");
        }
        throw std::runtime_error("The session id is not sequential");
    }
    void request_terminate() {
        terminate_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_request_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_requested_.notify_one();
        }
        s_terminated_.wait();
    }
    bool is_terminated() noexcept { return terminate_; }
    void confirm_terminated() { s_terminated_.post(); }

private:
    std::atomic_ulong requested_{0};
    std::atomic_ulong accepted_{0};
    std::atomic_bool wait_for_request_{};
    std::atomic_bool wait_for_accept_{};
    std::atomic_bool terminate_{};
    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition c_requested_{};
    boost::interprocess::interprocess_condition c_accepted_{};
    boost::interprocess::interprocess_semaphore s_terminated_{0};
};

};  // namespace tateyama::common
