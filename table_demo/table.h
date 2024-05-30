#pragma once
#include <atomic>
#include <cassert>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>
#ifndef NTOS_TABLES_H
#define NTOS_TABLES_H
#include "inlineskiplist.h"

namespace table {

template <typename T>
inline auto MakeTable() {}

template <typename T>
class SelectHelper {
   public:
    SelectHelper() = default;

    template <typename U, typename Func>
    SelectHelper<T> Where(Func&&);

   private:
};


template <typename T>
class Row {
    struct RowData {
        uint64_t version_{0};
        bool is_deleted_{false};
        T data_{};
        std::atomic<RowData*> next_{nullptr};
        std::atomic<RowData*> pre_{nullptr};

        explicit RowData(T&& new_data) : data_(std::move(new_data)) {}
        RowData(T&& new_data, uint64_t new_version)
            : version_(new_version), data_(std::move(new_data)) {}
        RowData(uint64_t new_version, bool deleted)
            : version_(new_version), is_deleted_(deleted) {}
    };

   public:
    Row(uint64_t id, T&& data, uint64_t version)
        : id_(id), latest_version_(version), oldest_version_(version) {
        head_.store(new T(std::move(data), version), std::memory_order_relaxed);
        tail_.store(head_, std::memory_order_relaxed);
    }

    const T* Get(uint64_t read_version);
    bool Update(T&& data, uint64_t version, bool do_delete);
    bool Commit(uint64_t version);
    uint64_t Compact(uint64_t version);

   private:
    uint64_t id_;
    std::atomic_uint64_t latest_version_;
    std::atomic_uint64_t oldest_version_;
    std::atomic<RowData*> head_;
    std::atomic<RowData*> tail_;
    std::atomic<RowData*> uncommit_head_;
    std::atomic<RowData*> uncommit_tail_;
};

template <typename T>
class RowSet {
    friend class SelectHelper<T>;

   public:
    struct Iterator {
        bool Next();
        const T* Get();

       private:
        RowSet<T>* rowset_;
        std::atomic<size_t> index_{0};
        std::atomic<bool> is_start{false};
    };

    const T* Get(size_t i, uint64_t read_version) const {
        return rows_[i]->Get(read_version);
    }
    const T* Get(size_t i) const { return rows_[i]->Get(commit_version_); }
    const T* operator[](size_t i) const {
        return rows_[i]->Get(commit_version_);
    }

    Iterator Iter() const { return Iterator{this}; }
    size_t Size() const { return rows_.size(); }

   private:
    RowSet(uint64_t commit_version, std::vector<Row<T>*> rows)
        : commit_version_(commit_version), rows_(rows) {}

   private:
    uint64_t commit_version_;
    std::vector<Row<T>*> rows_;
};

template <typename T>
class Table {
   public:
    explicit Table(std::string_view name) : table_name_(name) {}

    template <typename U = T>
    bool Update(uint64_t id, U&& data);

    template <typename U = T>
    bool Delete(uint64_t id, U&& data);

    RowSet<T> Select(uint64_t id);

   private:
    std::string table_name_;
    std::atomic<uint64_t> commit_version_{0};
    std::atomic<uint64_t> alloc_version_{0};
    SkipList<uint64_t, Row<T>> index_;
};

template <typename T>
const T* Row<T>::Get(uint64_t read_version) {
    auto latest_version = latest_version_.load(std::memory_order_relaxed);
    if (read_version >= latest_version) {
        return &(tail_.load(std::memory_order_acquire)->data_);
    }

    auto oldest_version = oldest_version_.load(std::memory_order_relaxed);
    if (read_version == oldest_version) {
        return &(tail_.load(std::memory_order_acquire)->data_);
    }

    if (read_version < oldest_version) {
        return nullptr;
    }

    if (read_version < (latest_version + oldest_version) / 2) {
        auto next = head_.load(std::memory_order_relaxed)
                        ->next_.load(std::memory_order_relaxed);
        auto current = head_.load(std::memory_order_relaxed);
        while (next != nullptr && next->version <= read_version) {
            current = next;
            next = next->next_.load(std::memory_order_relaxed);
        }
        assert(current != nullptr);
        return &(current->data_);
    } else {
        auto prev = tail_.load(std::memory_order_relaxed)
                        ->pre_.load(std::memory_order_relaxed);
        while (prev != nullptr && prev->version > read_version) {
            prev = prev->pre_.load(std::memory_order_relaxed);
        }
        return prev == nullptr ? nullptr : &(prev->data_);
    }
}

template <typename T>
bool Row<T>::Update(T&& data, uint64_t version, bool do_delete) {
    if (version <= latest_version_.load()) {
        return false;
    }
    RowData* new_data;
    if (do_delete) {
        new_data = new RowData(version, true);
    } else {
        new_data = new RowData(std::move(data), version);
    }

    auto current_tail = tail_.load(std::memory_order_acquire);
    while (current_tail != new_data) {
        new_data->pre_ = current_tail;
        if (tail_.compare_exchange_weak(current_tail, new_data)) {
            current_tail->next_ = new_data;
            break;
        }
    }

    auto current_latest_version =
        latest_version_.load(std::memory_order_acquire);
    while (current_latest_version < version) {
        if (latest_version_.compare_exchange_weak(current_latest_version,
                                                  version)) {
            break;
        }
    }
    return true;
}

template <typename T>
bool Row<T>::Commit(uint64_t version) {}

template <typename T>
uint64_t Row<T>::Compact(uint64_t version) {
    auto next = head_.load(std::memory_order_relaxed)
                    ->next_.load(std::memory_order_relaxed);
    auto current = head_.load(std::memory_order_relaxed);
    while (next != nullptr && next->version <= version) {
        current = next;
        next = next->next_.load(std::memory_order_relaxed);
    }
    assert(current != nullptr);
    auto prev = current->pre_.load();
    current->pre_.store(nullptr, std::memory_order_release);
    head_.store(current, std::memory_order_release);
    oldest_version_.store(current->version_, std::memory_order_relaxed);
    while (prev != nullptr) {
        auto prev_tmp = prev;
        delete prev;
        prev = prev_tmp;
    }
    return current->version_;
}

template <typename T>
bool RowSet<T>::Iterator::Next() {
    if (!is_start.load(std::memory_order_relaxed)) {
        index_.fetch_add(1);
    }
    return index_.load() < rowset_->Size();
}
template <typename T>
const T* RowSet<T>::Iterator::Get() {
    return rowset_->Get(index_.load(std::memory_order_relaxed));
}

template <typename T>
template <typename U>
bool Table<T>::Update(uint64_t id, U&& data) {
    T* row;
    auto r = index_.Find(id, &row);
    auto version = alloc_version_.fetch_add(1);
    if (r) {
        row->Update(std::move());
    }
}

template <typename T>
template <typename U>
bool Table<T>::Delete(uint64_t id, U&& data) {}

template <typename T>
RowSet<T> Table<T>::Select(uint64_t id) {}

}  // namespace table

#endif
