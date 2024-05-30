#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <execution>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <queue>
#include <ratio>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

// todo 改成引用计数，和垃圾回收
template <typename T>
class DoubleLinkdeList {
    struct Node {
        const size_t uid_;
        T data_;
        std::atomic<Node*> next_{nullptr};
        std::atomic<Node*> prev_{nullptr};
        Node(size_t uid, T&& data) : uid_(uid), data_(std::move(data)) {}
        Node(size_t uid, const T& data) : uid_(uid), data_(data) {}
    };

   public:
    DoubleLinkdeList() = default;

    template <typename U = T>
    U* PushBack(U&& data);

    template <typename U = T>
    U* PushFront(U&& data);

    T* PopFront();
    T* PopBack();

    template <typename U = T>
    U* Insert(const Node* prev_node, U&& data);

    T* Head();
    T* Tail();

    T* Remove(const Node* begin, const Node* end);

    T* RemoveBefore(const Node* end);
    T* RemoveAfter(const Node* begin);

    struct Iterator {
        bool Next();
        bool Prev();
        T* Get();
        const Node* GetNode() const;
        void SeekToLast();
        void SeekToFirst();
        void SeekTo(std::function<bool(const T&)>);

       private:
        std::atomic<Node*> node_;
        DoubleLinkdeList<T>* list_;
    };

   private:
    friend struct Iterator;
    std::atomic<Node*> head_{nullptr};
    std::atomic<Node*> tail_{nullptr};
    std::atomic<size_t> size_{0};
    const size_t uid_{std::hash<const void*>()(static_cast<const void*>(this))};
};
template <typename T>
template <typename U>
U* DoubleLinkdeList<T>::PushBack(U&& data) {
    auto node = new Node(std::forward<U>(data));
    auto tail = tail_.load(std::memory_order_acquire);
    node->prev_.store(tail, std::memory_order_release);
    while (!tail_.compare_exchange_weak(tail, node)) {
        node->prev_.store(tail, std::memory_order_release);
    }
    if (tail != nullptr) {
        tail->next_.store(node, std::memory_order_release);
    } else {
        head_.compare_exchange_strong(nullptr, node);
    }
    size_.fetch_add(1, std::memory_order_relaxed);
    return &(node->data_);
}

template <typename T>
template <typename U>
U* DoubleLinkdeList<T>::PushFront(U&& data) {
    auto node = new Node(std::forward<U>(data));
    auto head = head_.load(std::memory_order_acquire);
    node->next_.store(head, std::memory_order_release);
    while (!head_.compare_exchange_weak(head, node)) {
        node->next_.store(head, std::memory_order_release);
    }
    if (head != nullptr) {
        head->prev_.store(node, std::memory_order_release);
    } else {
        tail_.compare_exchange_strong(nullptr, node);
    }
    size_.fetch_add(1, std::memory_order_relaxed);
    return &(node->data_);
}

template <typename T>
T* DoubleLinkdeList<T>::PopFront() {
    auto head = head_.load(std::memory_order_acquire);
    if (head == nullptr) {
        return nullptr;
    }
    auto next = head->next_.load();
    while (!head_.compare_exchange_weak(head, next)) {
        next = head->next_.load();
    }
    size_.fetch_sub(1, std::memory_order_relaxed);
    delete head;
    if (next != nullptr) {
        next->prev_.compare_exchange_strong(head, nullptr);
        return &(next->data_);
    } else {
        if (tail_.compare_exchange_strong(head, nullptr)) {
            return nullptr;

        } else {
            return &(head->data_);
        }
    }
}

template <typename T>
T* DoubleLinkdeList<T>::PopBack() {
    auto tail = tail_.load(std::memory_order_acquire);
    if (tail == nullptr) {
        return nullptr;
    }
    auto prev = tail->prev_.load();
    while (!tail_.compare_exchange_weak(tail, prev)) {
        prev = tail->prev_.load();
    }
    size_.fetch_sub(1, std::memory_order_relaxed);
    delete tail;
    if (prev != nullptr) {
        prev->next_.compare_exchange_strong(tail, nullptr);
        return &(prev->data_);
    } else {
        if (head_.compare_exchange_strong(tail, nullptr)) {
            return nullptr;

        } else {
            return &(tail->data_);
        }
    }
}

template <typename T>
template <typename U>
U* DoubleLinkdeList<T>::Insert(const Node* prev_node, U&& data) {
    assert(prev_node->uid_ == uid_);
    auto node = new Node(std::forward<U>(data));
    auto next_node = prev_node->next_.load(std::memory_order_acquire);
    node->next_.store(next_node, std::memory_order_release);
    node->prev_.store(prev_node, std::memory_order_release);
    while (!prev_node->next_.compare_exchange_weak(next_node, node)) {
        node->next_.store(next_node, std::memory_order_release);
    }

    if (next_node != nullptr) {
        while (true) {
            auto tmp_node = prev_node;
            if (next_node->prev_.compare_exchange_weak(prev_node, node)) {
                break;
            }
            prev_node = tmp_node;
        }
    } else {
        tail_.compare_exchange_strong(nullptr, node);
    }
    size_.fetch_add(1, std::memory_order_relaxed);
    return &(node->data_);
}

template <typename T>
T* DoubleLinkdeList<T>::Head() {
    return &(head_.load(std::memory_order_relaxed)->data_);
}
template <typename T>
T* DoubleLinkdeList<T>::Tail() {
    return &(tail_.load(std::memory_order_relaxed)->data_);
}

template <typename T>
T* DoubleLinkdeList<T>::Remove(const Node* begin, const Node* end) {
    // todo 需要判断 begin 是否在 end 前面，不然会成环。
    assert(begin != nullptr, end != nullptr);
    assert(begin->uid_ == uid_ && end->uid_ == uid_);

    auto begin_next = begin->next_.load(std::memory_order_acquire);
    while (!begin->next_.compare_exchange_weak(begin_next, end)) {
    }
}

template <typename T>
T* DoubleLinkdeList<T>::RemoveBefore(const Node* end) {}
template <typename T>
T* DoubleLinkdeList<T>::RemoveAfter(const Node* begin) {}

int main() {
    // Test test{0, 0};
    // TestPadding test_padding{.a = 0, .b = 0};
    // auto now = std::chrono::system_clock::now();
    // std::thread t1(RunA<Test>, std::ref(test));
    // std::thread t2(RunB<Test>, std::ref(test));
    // t1.join();
    // t2.join();
    // auto end = std::chrono::system_clock::now();
    // auto duration = end - now;
    // auto duration_ms =
    //     std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    // std::cout << "no padding, cost " << duration_ms.count() << "ms"
    //           << std::endl;

    // now = std::chrono::system_clock::now();
    // std::thread t3(RunA<TestPadding>, std::ref(test_padding));
    // std::thread t4(RunB<TestPadding>, std::ref(test_padding));
    // t3.join();
    // t4.join();
    // end = std::chrono::system_clock::now();
    // duration = end - now;
    // duration_ms =
    //     std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    // std::cout << "padding, cost " << duration_ms.count() << "ms" <<
    // std::endl;

    return 0;
}
