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
#include <atomic>
#include <stdexcept> // std::runtime_error
#include <vector>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
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

    message_header(index_type idx, length_type length) : idx_(idx), length_(length) {}
    message_header() : message_header(0, 0) {}
    explicit message_header(const char* buffer) {
        std::memcpy(&idx_, buffer, sizeof(index_type));
        std::memcpy(&length_, buffer + sizeof(index_type), sizeof(length_type));  //NOLINT
    }

    [[nodiscard]] length_type get_length() const { return length_; }
    [[nodiscard]] index_type get_idx() const { return idx_; }
    char* get_buffer() {
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
    explicit length_header(const char* buffer) {
        std::memcpy(&length_, buffer, sizeof(length_type));
    }

    [[nodiscard]] length_type get_length() const { return length_; }
    char* get_buffer() {
        std::memcpy(buffer_, &length_, sizeof(length_type));  //NOLINT
        return static_cast<char*>(buffer_);
    };

private:
    length_type length_{};
    char buffer_[size]{};  //NOLINT
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
    static constexpr std::size_t Alignment = 64;

    /**
     * @brief Construct a new object.
     */
    simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : capacity_(capacity) {
        auto buffer = static_cast<char*>(managed_shm_ptr->allocate_aligned(capacity_, Alignment));
        buffer_handle_ = managed_shm_ptr->get_handle_from_address(buffer);
    }

    /**
     * @brief Construct a new object for result_set_wire.
     */
    simple_wire() {
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
            if(stored() >= T::size) {
                break;
            }
            if (wait_flag) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                wait_for_read_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                c_empty_.wait(lock, [this](){ return stored() >= T::size; });
                wait_for_read_ = false;
            } else {
                if (stored() < T::size) { return T(); }
            }
        }
        if ((base + capacity_) >= (read_address(base) + sizeof(T))) {  //NOLINT
            T header(read_address(base));  // normal case
            return header;
        }
        char buf[sizeof(T)];  // in case for ring buffer full  //NOLINT
        std::size_t first_part = capacity_ - index(poped_);
        memcpy(buf, read_address(base), first_part);  //NOLINT
        memcpy(buf + first_part, base, sizeof(T) - first_part);  //NOLINT
        T header(static_cast<char*>(buf));
        return header;
    }
    T peep(bool wait_flag = false) {
        return peep(get_bip_address(), wait_flag);
    }

    char* get_bip_address(boost::interprocess::managed_shared_memory* managed_shm_ptr) {
        if (buffer_handle_ != 0) {
            return static_cast<char*>(managed_shm_ptr->get_address_from_handle(buffer_handle_));
        }
        return nullptr;
    }

    // for ipc_request
    [[nodiscard]] std::size_t read_point() const { return poped_; }

protected:
    std::size_t stored() const { return (pushed_ - poped_); }  //NOLINT
    std::size_t room() const { return capacity_ - stored(); }  //NOLINT
    std::size_t index(std::size_t n) const { return n %  capacity_; }  //NOLINT
    char* buffer_address(char* base, std::size_t n) { return base + index(n); }  //NOLINT
    const char* read_address(const char* base, std::size_t offset) const { return base + index(poped_ + offset); }  //NOLINT
    const char* read_address(const char* base) const { return base + index(poped_); }  //NOLINT
    void wait_to_write(std::size_t length) {
        boost::interprocess::scoped_lock lock(m_mutex_);
        wait_for_write_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        c_full_.wait(lock, [this, length](){ return room() >= length; });
        wait_for_write_ = false;
    }
    void write_in_buffer(char *base, char* to, const char* from, std::size_t length) {
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

    boost::interprocess::managed_shared_memory::handle_t buffer_handle_{};  //NOLINT
    std::size_t capacity_;  //NOLINT

    std::atomic_ulong pushed_{0};  //NOLINT
    std::size_t poped_{0};  //NOLINT

    std::atomic_bool wait_for_write_{};  //NOLINT
    std::atomic_bool wait_for_read_{};  //NOLINT

    boost::interprocess::interprocess_mutex m_mutex_{};  //NOLINT
    boost::interprocess::interprocess_condition c_empty_{};  //NOLINT
    boost::interprocess::interprocess_condition c_full_{};  //NOLINT
};


// for request
class unidirectional_message_wire : public simple_wire<message_header> {
public:
    unidirectional_message_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : simple_wire<message_header>(managed_shm_ptr, capacity) {}

    /**
     * @brief push a request message into the queue.
     */
    void write(char* base, const char* from, message_header&& header) {
        std::size_t length = header.get_length() + message_header::size;
        if (length > room()) { wait_to_write(length); }
        write_in_buffer(base, buffer_address(base, pushed_), header.get_buffer(), message_header::size);
        write_in_buffer(base, buffer_address(base, pushed_ + message_header::size), from, header.get_length());
        pushed_ += length;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }

    /**
     * @brief get the address of the first request message in the queue.
     */
    const char* payload(const char* base, std::size_t length) {
        if (index(poped_ + message_header::size) < index(poped_ + message_header::size + length)) {
            return read_address(base, message_header::size);
        }
        copy_of_payload_ = std::make_unique<char[]>(length);  //NOLINT
        auto address = static_cast<char*>(copy_of_payload_.get());
        read_from_buffer(address, base, read_address(base, message_header::size), length);
        return address;  // ring buffer wrap around case
    }

    /**
     * @brief pop the current message.
     */
    void read(char* to, const char* base, std::size_t length) {
        read_from_buffer(to, base, read_address(base, message_header::size), length);
        poped_ += message_header::size + length;
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
    void dispose(const char* base, const std::size_t read_point) {
        if (poped_ == read_point) {
            poped_ += (peep(base).get_length() + message_header::size);
            if (copy_of_payload_) {
                copy_of_payload_ = nullptr;
            }
            return;
        }
        //  FIXME (Processing when a message located later is discarded first)
    }

private:
    std::unique_ptr<char[]> copy_of_payload_{};  // in case of ring buffer wrap around  //NOLINT
};


// for response
class response_box {
public:

    class response {
        friend class response_box;

    public:
        static constexpr std::size_t max_response_message_length = 256;

        response() = default;
        ~response() = default;

        /**
         * @brief Copy and move constructers are deleted.
         */
        response(response const&) = delete;
        response(response&&) = delete;
        response& operator = (response const&) = delete;
        response& operator = (response&&) = delete;

        std::pair<char*, std::size_t> recv(long timeout = 0) {
            if (!(written_ > read_)) {
                if (timeout == 0) {
                    boost::interprocess::scoped_lock lock(m_restored_);
                    w_restored_ = true;
                    std::atomic_thread_fence(std::memory_order_acq_rel);
                    c_restored_.wait(lock, [this](){ return written_ > read_; });
                    w_restored_ = false;
                } else {
                    boost::interprocess::scoped_lock lock(m_restored_);
                    w_restored_ = true;
                    std::atomic_thread_fence(std::memory_order_acq_rel);
                    if (!c_restored_.timed_wait(lock,
#ifdef BOOST_DATE_TIME_HAS_NANOSECONDS
                                                boost::get_system_time() + boost::posix_time::nanoseconds(timeout),
#else
                                                boost::get_system_time() + boost::posix_time::microseconds((timeout+500)/1000),
#endif
                                                [this](){ return written_ > read_; })) {
                        throw std::runtime_error("response has not been received within the specified time");
                    }
                    w_restored_ = false;
                }
            }
            if (read_ == 0) {
                if (annex_ != 0) {
                    return std::pair<char*, std::size_t>(static_cast<char*>(client_managed_shm_ptr_->get_address_from_handle(annex_)), annex_length_);
                }
                return std::pair<char*, std::size_t>(static_cast<char*>(buffer_), length_);
            }
            if (second_annex_ != 0) {
                return std::pair<char*, std::size_t>(static_cast<char*>(client_managed_shm_ptr_->get_address_from_handle(second_annex_)), second_annex_length_);
            }
            return std::pair<char*, std::size_t>(static_cast<char*>(buffer_) + length_, second_length_);  // NOLINT
        }
        void set_inuse() {
            inuse_ = true;
        }
        [[nodiscard]] bool is_inuse() const { return inuse_; }
        void dispose() {
            if (++read_ == expected_) {
                length_ = second_length_ = 0;
                to_be_written_ = written_ = read_ = 0;
                expected_ = 1;
                inuse_ = false;
                query_ = false;
                if (annex_ != 0) {
                    client_managed_shm_ptr_->deallocate(client_managed_shm_ptr_->get_address_from_handle(annex_));
                    annex_ = 0;
                    annex_length_ = 0;
                }
                if (second_annex_ != 0) {
                    client_managed_shm_ptr_->deallocate(client_managed_shm_ptr_->get_address_from_handle(second_annex_));
                    second_annex_ = 0;
                    second_annex_length_ = 0;
                }
            }
        }
        char* get_buffer(std::size_t length) {
            if (to_be_written_++ == 0) {
                if (length <= max_response_message_length) {
                    length_ = length;
                    return static_cast<char*>(buffer_);
                }
                annex_ = allocate_buffer(length);
                annex_length_ = length;
                return static_cast<char*>(server_managed_shm_ptr_->get_address_from_handle(annex_));
            }
            if (second_length_ <= (max_response_message_length - length_)) {
                second_length_ = length;
                return static_cast<char*>(buffer_) + length_;  // NOLINT
            }
            second_annex_ = allocate_buffer(length);
            second_annex_length_ = length;
            return static_cast<char*>(server_managed_shm_ptr_->get_address_from_handle(second_annex_));
        }
        void flush() {
            written_++;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (w_restored_) {
                boost::interprocess::scoped_lock lock(m_restored_);
                c_restored_.notify_one();
            }
        }
        void set_query_mode() {
            expected_ = 2;
            query_ = true;
        }
        void un_receive() {
            expected_--;
            read_--;
        }
    private:
        static constexpr std::size_t Alignment = sizeof(std::size_t);

        void set_server_managed_shared_memory(boost::interprocess::managed_shared_memory* managed_shm_ptr) {
            server_managed_shm_ptr_ = managed_shm_ptr;
        }
        void set_client_managed_shared_memory(boost::interprocess::managed_shared_memory* managed_shm_ptr) {
            client_managed_shm_ptr_ = managed_shm_ptr;
        }
        boost::interprocess::managed_shared_memory::handle_t allocate_buffer(std::size_t length) {
            auto buffer = static_cast<char*>(server_managed_shm_ptr_->allocate_aligned(length, Alignment));
            return server_managed_shm_ptr_->get_handle_from_address(buffer);
        }

        std::size_t length_{};
        std::size_t second_length_{};
        unsigned expected_{1};
        unsigned to_be_written_{};
        std::atomic_uint written_{};
        unsigned read_{};
        bool inuse_{};
        bool query_{};

        boost::interprocess::managed_shared_memory* server_managed_shm_ptr_{};
        boost::interprocess::managed_shared_memory* client_managed_shm_ptr_{};
        boost::interprocess::interprocess_mutex m_restored_{};
        boost::interprocess::interprocess_condition c_restored_{};
        std::atomic_bool w_restored_{};
        char buffer_[max_response_message_length]{};  //NOLINT
        boost::interprocess::managed_shared_memory::handle_t annex_{};
        std::size_t annex_length_{};
        boost::interprocess::managed_shared_memory::handle_t second_annex_{};
        std::size_t second_annex_length_{};
    };

    /**
     * @brief Construct a new object.
     */
    explicit response_box(size_t aSize, boost::interprocess::managed_shared_memory* managed_shm_ptr) : boxes_(aSize, managed_shm_ptr->get_segment_manager()) {
        for (auto &&r: boxes_) {
            r.set_server_managed_shared_memory(managed_shm_ptr);
        }
    }
    response_box() = delete;
    ~response_box() = default;

    /**
     * @brief Copy and move constructers are deleted.
     */
    response_box(response_box const&) = delete;
    response_box(response_box&&) = delete;
    response_box& operator = (response_box const&) = delete;
    response_box& operator = (response_box&&) = delete;

    std::size_t size() { return boxes_.size(); }
    response& at(std::size_t idx) { return boxes_.at(idx); }
    void connect(boost::interprocess::managed_shared_memory* managed_shm_ptr) {
        for (auto &&r: boxes_) {
            r.set_client_managed_shared_memory(managed_shm_ptr);
        }
    }

private:
    std::vector<response, boost::interprocess::allocator<response, boost::interprocess::managed_shared_memory::segment_manager>> boxes_;
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
            write(get_bip_address(managed_shm_ptr_), from, length);
        }

        /**
         * @brief provide the current chunk to MsgPack.
         */
        std::pair<char*, std::size_t> get_chunk(char* base, long timeout = 0) {
            if (chunk_end_ < poped_) {
                chunk_end_ = poped_;
            }
            if (!(chunk_end_ < pushed_)) {
                if (timeout == 0) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    wait_for_column_ = true;
                    std::atomic_thread_fence(std::memory_order_acq_rel);
                    c_empty_.wait(lock, [this](){ return chunk_end_ < pushed_; });
                    wait_for_column_ = false;
                } else {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    wait_for_column_ = true;
                    std::atomic_thread_fence(std::memory_order_acq_rel);
                    if (!c_empty_.timed_wait(lock,
#ifdef BOOST_DATE_TIME_HAS_NANOSECONDS
                                             boost::get_system_time() + boost::posix_time::nanoseconds(timeout),
#else
                                             boost::get_system_time() + boost::posix_time::microseconds((timeout+500)/1000),
#endif
                                             [this](){ return chunk_end_ < pushed_; })) {
                        throw std::runtime_error("record has not been received within the specified time");
                    }
                    wait_for_column_ = false;
                }
            }
            auto chunk_start = chunk_end_;
            if ((pushed_ / capacity_) == (chunk_start / capacity_)) {
                chunk_end_ = pushed_;
            } else {
                chunk_end_ = (pushed_ / capacity_) * capacity_;
            }
            return std::pair<char*, std::size_t>(buffer_address(base, chunk_start), chunk_end_ - chunk_start);
        }
        /**
         * @brief dispose of data that has completed read and is no longer needed
         */
        void dispose(std::size_t length) {
            poped_ += length;
            chunk_end_ = 0;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            if (wait_for_write_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_full_.notify_one();
            }
        }
        /**
         * @brief check this wire has record
         */
        [[nodiscard]] bool has_record() const { return pushed_ > poped_; }

        void attach_buffer(boost::interprocess::managed_shared_memory::handle_t handle, std::size_t capacity) {
            buffer_handle_ = handle;
            capacity_ = capacity;
        }
        void detach_buffer() {
            buffer_handle_ = 0;
            capacity_ = 0;
        }
        bool equal(boost::interprocess::managed_shared_memory::handle_t handle) {
            return handle == buffer_handle_;
        }

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
                write_in_buffer(base, buffer_address(base, pushed_), from, length);
                pushed_ += length;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                if (wait_for_column_) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    c_empty_.notify_one();
                } else {
                    envelope_->notify_record_arrival();
                }
            }
        }

        void set_closed() {
            closed_ = true;
            if (wait_for_write_) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                c_full_.notify_one();
            }
        }

        void set_environments(unidirectional_simple_wires* envelope, boost::interprocess::managed_shared_memory* managed_shm_ptr) {
            envelope_ = envelope;
            managed_shm_ptr_ = managed_shm_ptr;
        }

        std::size_t chunk_end_{0};

        boost::interprocess::managed_shared_memory* managed_shm_ptr_{};  // used by server only
        std::atomic_bool wait_for_column_{};
        std::atomic_bool closed_{};
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
    }
    ~unidirectional_simple_wires() {
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
        }
        auto index = search_free_wire();
        unidirectional_simple_wires_.at(index).attach_buffer(managed_shm_ptr_->get_handle_from_address(buffer), wire_size);
        only_one_buffer_ = false;
        return &unidirectional_simple_wires_.at(index);
    }
    void release(unidirectional_simple_wire* wire) {
        char* buffer = wire->get_bip_address(managed_shm_ptr_);

        if (reserved_ == nullptr) {
            reserved_ = buffer;
        } else {
            managed_shm_ptr_->deallocate(buffer);
        }
        count_using_--;
    }

    unidirectional_simple_wire* active_wire(long timeout = 0) {
        do {
            for (auto&& wire: unidirectional_simple_wires_) {
                if(wire.has_record()) {
                    return &wire;
                }
            }
            if (timeout == 0) {
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
                                                boost::get_system_time() + boost::posix_time::microseconds((timeout+500)/1000),
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
    void set_closed() {
        for (auto&& wire: unidirectional_simple_wires_) {
            wire.set_closed();
        }
        closed_ = true;
    }
    [[nodiscard]] bool is_closed() const { return closed_; }

    /**
     * @brief mark the end of the result set by the sql service
     */
    void set_eor() {
        eor_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_record_) {
            boost::interprocess::scoped_lock lock(m_record_);
            c_record_.notify_one();
        }
    }
    [[nodiscard]] bool is_eor() const { return eor_; }

private:
    std::size_t search_free_wire() {
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

    /**
     * @brief notify the arrival of a record
     */
    void notify_record_arrival() {
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

    std::size_t request() {
        std::size_t rv = ++requested_;

        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_request_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_requested_.notify_one();
        }
        return rv;
    }
    bool check(std::size_t n, bool wait = false, long timeout = 0) {
        if (!wait) {
            return accepted_ >= n;
        }
        if (accepted_ >= n) {
            return true;
        }
        if (timeout == 0) {
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
                                        boost::get_system_time() + boost::posix_time::microseconds((timeout+500)/1000),
#endif
                                        [this, n](){ return (accepted_ >= n); })) {
                throw std::runtime_error("connection response has not been accepted within the specified time");
            }
            wait_for_accept_ = false;
        }
        return true;
    }
    std::size_t listen(bool wait = false) {
        if (accepted_ < requested_ || terminate_) {
            return accepted_ + 1;
        }
        if (!wait) {
            return 0;
        }
        {
            boost::interprocess::scoped_lock lock(m_mutex_);
            wait_for_request_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            c_requested_.wait(lock, [this](){ return (accepted_ < requested_) || terminate_; });
            wait_for_request_ = false;
        }
        return accepted_ + 1;
    }
    void accept(std::size_t n) {
        if (n == (accepted_ + 1)) {
            if (n <= requested_) {
                accepted_ = n;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                if (wait_for_accept_) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    c_accepted_.notify_all();
                }
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
    bool is_terminated() { return terminate_; }
    void confirm_terminated() { s_terminated_.post(); }

private:
    std::size_t requested_{0};
    std::size_t accepted_{0};
    std::atomic_bool wait_for_request_{};
    std::atomic_bool wait_for_accept_{};
    std::atomic_bool terminate_{};
    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition c_requested_{};
    boost::interprocess::interprocess_condition c_accepted_{};
    boost::interprocess::interprocess_semaphore s_terminated_{0};
};

};  // namespace tateyama::common
