#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <execution>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <ostream>
#include <queue>
#include <ratio>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

template <typename T>
class RefCount {
   public:
    explicit RefCount(T* ptr) : ptr_(ptr) {}
    RefCount(const RefCount&) = delete;
    RefCount(RefCount&&) = delete;
    RefCount& operator=(const RefCount&) = delete;
    RefCount& operator=(RefCount&&) = delete;
    ~RefCount() = default;
    void Ref() noexcept;
    void UnRef() noexcept;

   private:
    void Dispose() noexcept;
    void Destroy() noexcept;

    T* ptr_;
    std::atomic<uint64_t> ref_{1};
};

template <typename T>
void RefCount<T>::Ref() noexcept {
    ref_.fetch_add(1, std::memory_order_relaxed);
}

template <typename T>
void RefCount<T>::UnRef() noexcept {
    if (ref_.fetch_sub(1) == 1) {
        Dispose();
        Destroy();
    }
}
template <typename T>
void RefCount<T>::Dispose() noexcept {
    delete ptr_;
}
template <typename T>
void RefCount<T>::Destroy() noexcept {
    delete this;
}

template <typename T>
class DoubleLinkdeList {
   public:
    class NodeData;

   private:
    struct Node {
        explicit Node(size_t uid) noexcept
            : uid_(uid), ref_count_(nullptr), has_value(false) {}
        Node(size_t uid, T&& data) noexcept
            : uid_(uid), ref_count_(new RefCount<Node>(this)) {
            new (&data_) T(std::move(data));
        }
        Node(size_t uid, const T& data) noexcept
            : uid_(uid), ref_count_(new RefCount<Node>(this)) {
            new (&data_) T(data);
        }
        Node(const Node&) = delete;
        Node(Node&&) = delete;
        Node& operator=(const Node&) = delete;
        Node& operator=(Node&&) = delete;
        ~Node() {
            if (has_value) {
                data_ptr()->~T();
            }
        }

        void Release() noexcept;
        NodeData GetData() const noexcept;

        size_t uid_;
        RefCount<Node>* ref_count_;
        std::atomic<Node*> next_{nullptr};
        std::atomic<Node*> prev_{nullptr};
        bool has_value{true};
        mutable std::aligned_storage_t<sizeof(T), alignof(T)> data_;

       private:
        inline T* data_ptr() const { return reinterpret_cast<T*>(&data_); }
    };

   public:
    class NodeData {
       public:
        NodeData() = default;
        NodeData(const T* ptr, RefCount<Node>* ref_count) noexcept
            : ptr_(ptr), ref_count_(ref_count), has_value_(true) {
            ref_count_->Ref();
        }
        ~NodeData() noexcept {
            if (ref_count_) {
                ref_count_->UnRef();
            }
        }
        NodeData(const NodeData& o) noexcept
            : ptr_(o.ptr_), ref_count_(o.ref_count_), has_value_(o.has_value_) {
            if (ref_count_) {
                o.ref_count_->Ref();
            }
        }
        NodeData(NodeData&& o) noexcept
            : ptr_(o.ptr_), ref_count_(o.ref_count_), has_value_(o.has_value_) {
            o.ptr_ = nullptr;
            o.ref_count_ = nullptr;
        }
        NodeData& operator=(const NodeData& o) noexcept {
            if (&o == this) {
                return *this;
            }
            if (ref_count_) {
                o.ref_count_->Ref();
            }
            ptr_ = o.ptr_;
            ref_count_ = o.ref_count_;
            has_value_ = o.has_value_;
            return *this;
        }
        NodeData& operator=(NodeData&& o) noexcept {
            if (&o == this) {
                return *this;
            }
            ptr_ = o.ptr_;
            ref_count_ = o.ref_count_;
            has_value_ = o.has_value_;
            o.ptr_ = nullptr;
            o.ref_count_ = nullptr;
            o.has_value_ = false;
            return *this;
        }
        const T* Get() const { return ptr_; }
        bool HasValue() const { return has_value_; }

       private:
        const T* ptr_{nullptr};
        RefCount<Node>* ref_count_{nullptr};
        bool has_value_{false};
    };

    struct Iterator {
        bool Next();
        NodeData Get();
        Node* CherryPickNode();
        void SeekToLast();
        void SeekToFirst();
        void SeekTo(std::function<bool(const T&)>);

        explicit Iterator(DoubleLinkdeList<T>* list) : list_(list) {
            SeekToFirst();
        }

       private:
        std::atomic<Node*> node_;
        DoubleLinkdeList<T>* list_;
    };

   public:
    DoubleLinkdeList()
        : uid_(std::hash<const void*>()(static_cast<const void*>(this))),
          head_(uid_),
          tail_(uid_) {
        head_.next_.store(&tail_, std::memory_order_relaxed);
        tail_.prev_.store(&head_, std::memory_order_relaxed);
    }

    template <typename U = T>
    NodeData PushBack(U&& data);

    template <typename U = T>
    NodeData PushFront(U&& data);

    // todo
    // 删除存在生命周期的问题，两次pop同一个node，
    // 前一个删除后，后一个空悬。可以通过返回删除数据引用的方式延迟释放。
    // 但是批量删除的同时，写入节点到删除范围内，目前没有思路
    NodeData PopFront();
    NodeData PopBack();

    template <typename U = T>
    NodeData Insert(Node* prev_node, U&& data);
    NodeData Insert(Node* prev_node, Node* node);

    NodeData Head();
    NodeData Tail();

    template <bool need_release = true>
    NodeData Remove(Node* begin, Node* end);

    NodeData RemoveBefore(const Node* end);
    NodeData RemoveAfter(const Node* begin);
    size_t Size() const { return size_.load(std::memory_order_relaxed); }

    Iterator GetIter();

   private:
    friend struct Iterator;
    size_t uid_;
    Node head_;
    Node tail_;
    std::atomic<size_t> size_{0};
};

template <typename T>
void DoubleLinkdeList<T>::Node::Release() noexcept {
    ref_count_->UnRef();
}
template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::Node::GetData()
    const noexcept {
    if (has_value) {
        NodeData node_data(data_ptr(), ref_count_);
        return node_data;
    } else {
        return NodeData();
    }
}

template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::Insert(
    Node* prev_node, Node* node) {
    assert(prev_node != nullptr);
    assert(prev_node->uid_ == uid_);
    node->uid_ = uid_;
    auto next_node = prev_node->next_.load(std::memory_order_acquire);
    node->next_.store(next_node, std::memory_order_release);
    node->prev_.store(prev_node, std::memory_order_release);
    while (!prev_node->next_.compare_exchange_weak(next_node, node)) {
        node->next_.store(next_node, std::memory_order_relaxed);
    }

    while (true) {
        auto tmp_node = prev_node;
        if (next_node->prev_.compare_exchange_weak(prev_node, node)) {
            break;
        }
        prev_node = tmp_node;
    }
    size_.fetch_add(1, std::memory_order_relaxed);
    return node->GetData();
}

template <typename T>
template <typename U>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::Insert(
    Node* prev_node, U&& data) {
    Node* node = new Node(uid_, std::forward<U>(data));
    return Insert(prev_node, node);
}

template <typename T>
template <bool need_release>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::Remove(Node* begin,
                                                                   Node* end) {
    // todo 需要判断 begin 是否在 end 前面，不然会成环。
    assert(begin != nullptr && end != nullptr);
    assert(begin->uid_ == uid_ && end->uid_ == uid_);

    auto begin_prev = begin->prev_.load(std::memory_order_release);
    auto begin_tmp = begin;

    auto end_next = end->next_.load(std::memory_order_acquire);

    while (!begin_prev->next_.compare_exchange_weak(begin_tmp, end_next)) {
        begin_prev = begin->prev_.load(std::memory_order_release);
        begin_tmp = begin;
        if (begin_prev == nullptr) {
            return end_next->GetData();
        }
    }

    end_next->prev_.store(begin_prev, std::memory_order_release);
    begin->prev_.store(nullptr, std::memory_order_relaxed);
    end->next_.store(nullptr, std::memory_order_relaxed);

    while (begin != end) {
        if constexpr (need_release) {
            auto tmp = begin->next_.load();
            begin->Release();
            begin = tmp;
        } else {
            begin = begin->next_;
        }
        size_.fetch_sub(1, std::memory_order_relaxed);
    }
    if constexpr (need_release) {
        begin->Release();
    }
    size_.fetch_sub(1, std::memory_order_relaxed);
    return end_next->GetData();
}

template <typename T>
template <typename U>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::PushBack(U&& data) {
    return Insert(tail_.prev_.load(std::memory_order_acquire),
                  std::forward<U>(data));
}

template <typename T>
template <typename U>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::PushFront(
    U&& data) {
    return Insert(&head_, std::forward<U>(data));
}

template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::PopFront() {
    auto node = head_.next_.load(std::memory_order_relaxed);
    return Remove(node, node);
}

template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::PopBack() {
    auto node = tail_.prev_.load(std::memory_order_relaxed);
    return Remove(node, node);
}

template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::Head() {
    auto head = head_.next_.load(std::memory_order_relaxed);
    return head->GetData();
}
template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::Tail() {
    auto tail = tail_.prev_.load(std::memory_order_relaxed);
    return tail->GetData();
}

template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::RemoveBefore(
    const Node* end) {
    auto head = head_.next_.load(std::memory_order_relaxed);
    return remove(head, end);
}
template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::RemoveAfter(
    const Node* begin) {
    auto tail = tail_.prev_.load(std::memory_order_relaxed);
    return remove(begin, tail);
}

template <typename T>
bool DoubleLinkdeList<T>::Iterator::Next() {
    auto* node = node_.load();
    auto next_node = node->next_.load();
    while (!node_.compare_exchange_weak(node, next_node)) {
        node = node_.load();
        next_node = node->next_.load();
    }
    return next_node != &(list_->tail_);
}

template <typename T>
typename DoubleLinkdeList<T>::NodeData DoubleLinkdeList<T>::Iterator::Get() {
    return node_.load()->GetData();
}

template <typename T>
typename DoubleLinkdeList<T>::Node*
DoubleLinkdeList<T>::Iterator::CherryPickNode() {
    auto* node = node_.load();
    list_->Remove<false>(node, node);
    return node;
}

template <typename T>
void DoubleLinkdeList<T>::Iterator::SeekToLast() {
    node_.store(&(list_->tail_));
}

template <typename T>
void DoubleLinkdeList<T>::Iterator::SeekToFirst() {
    node_.store(&(list_->head_));
}

template <typename T>
void DoubleLinkdeList<T>::Iterator::SeekTo(std::function<bool(const T&)> func) {
    SeekToFirst();
    auto next_node = node_.load()->next_.load();
    while (next_node != &list_->tail_) {
        auto node_data = next_node->GetData();
        if (func(*node_data.Get())) {
            node_.store(next_node);
            return;
        }
    }
}

template <typename T>
typename DoubleLinkdeList<T>::Iterator DoubleLinkdeList<T>::GetIter() {
    return Iterator(this);
}

void push(DoubleLinkdeList<int>* list, int id, int all) {
    std::cout << "t: " << id << std::endl;
    for (int i = 0; i < 10; i++) {
        list->PushBack(i * all + id);
    }
}

void pop(DoubleLinkdeList<int>* list) {
    for (int i = 0; i < 10000; i++) {
        list->PopBack();
    }
}

int main() {
    DoubleLinkdeList<int> list;

    std::vector<std::thread> pool;
    int thread_num = 10;
    pool.reserve(thread_num);
    for (int i = 0; i < thread_num; ++i) {
        pool.emplace_back(push, &list, i, thread_num);
    }

    for (auto&& t : pool) {
        t.join();
    }

    std::cout << "size " << list.Size() << std::endl;

    std::vector<std::thread> pool2;
    int thread2_num = 10;
    pool2.reserve(thread2_num);
    for (int i = 0; i < thread2_num; ++i) {
        pool2.emplace_back(pop, &list);
    }

    for (auto&& t : pool2) {
        t.join();
    }

    std::cout << "size " << list.Size() << std::endl;

    // auto it = list.GetIter();
    // while (it.Next()) {
    //     std::cout << *(it.Get().Get()) << std::endl;
    // }

    return 0;
}
