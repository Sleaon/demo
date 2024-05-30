#pragma once
#include <cstdint>
#ifndef NTOS_TABLES_INLINESKIPLIST_H
#define NTOS_TABLES_INLINESKIPLIST_H
#include <atomic>
#include <cassert>
#include <cstring>
#include <memory>
#include <new>
#include <random>
#include <stdexcept>
#include <type_traits>

#if defined(__aarch64__)
//  __builtin_prefetch(..., 1) turns into a prefetch into prfm pldl3keep. On
// arm64 we want this as close to the core as possible to turn it into a
// L1 prefetech unless locality == 0 in which case it will be turned into a
// non-temporal prefetch
#define PREFETCH(addr, rw, locality) \
    __builtin_prefetch(addr, rw, locality >= 1 ? 3 : locality)
#else
#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)
#endif

namespace table {

// 用于检测是否可拷贝
template <typename T>
using is_copyable =
    std::integral_constant<bool, std::is_copy_constructible_v<T> ||
                                     std::is_move_constructible_v<T> ||
                                     std::is_trivially_copyable_v<T>>;

// 用于检测类型是否满足比较运算符的约束
template <typename T, typename = void>
struct is_comparable : std::false_type {};

template <typename T>
struct is_comparable<
    T, std::void_t<decltype(std::declval<T>() < std::declval<T>()),
                   decltype(std::declval<T>() == std::declval<T>())>>
    : std::true_type {};

// 用于检测类型是否既可比较又可拷贝且不是指针
template <typename T>
constexpr bool is_comparable_and_copyable_v =
    !std::is_pointer_v<T> && is_comparable<T>::value && is_comparable<T>::value;

// 用于检测是否可拷贝
template <typename T>
constexpr bool is_copyable_v = is_copyable<T>::value;

template <typename Key, typename Value, typename Alloc = std::allocator<char>>
class SkipList {
    static_assert(
        is_comparable_and_copyable_v<Key>,
        "Key must be comparable and copyable, also can't be a pointer");

    static_assert(is_copyable_v<Value>, "Value must be copyable");

    static const uint16_t kMaxPossibleHeight = 32;
    struct Node;
    struct Splice;

   public:
    struct Position;
    explicit SkipList(uint32_t max_height = 12, float probability = 0.5);
    SkipList(const SkipList&) = delete;
    auto operator=(const SkipList&) = delete;
    SkipList(SkipList&&) noexcept;
    auto operator=(SkipList&&) noexcept;
    ~SkipList();

    bool Insert(const Key& key, const Value& value);
    bool InsertBySequence(const Key& key, const Value& value, Position& pos);

    bool Delete(const Key& key);
    bool DeleteBySequence(const Key& key, Position& pos);

    bool Contains(const Key& key) const;
    bool Find(const Key& key, Value** value);

    auto GetIterator();

    class Iterator {
       public:
        explicit Iterator(SkipList* list);

        void SetList(SkipList* list);

        bool Valid() const;
        const Key* key() const;
        Value* value();
        bool Next();
        bool Prev();

        bool Delete();

        void Seek(const Key& target);

        void SeekForPrev(const Key& target);

        void SeekToFirst();

        void SeekToLast();

       private:
        SkipList* list_;
        Node* node_;
    };

   private:
    bool Insert(const Key& key, const Value& value, Splice* splice,
                bool allow_partial_splice_fix);
    bool InsertWithHint(const Key& key, const Value& value, Splice** hint);
    bool Delete(Node* target_node, Splice* splice,
                bool allow_partial_splice_fix);
    bool DeleteWithHint(const Key key, Splice** hint);

    Node* AllocateData(const Key& key, const Value& value);
    Node* AllocateNode(uint32_t height);
    // Allocate a splice using allocator.
    Splice* AllocateSplice();
    template <bool NeedDestruct = true>
    void DeallocateData(Node* node);

    void DeallocateSplice(void* splice);

    inline uint32_t GetMaxHeight() const {
        return max_height_.load(std::memory_order_relaxed);
    }
    uint16_t RandomHeight();
    bool KeyIsAfterNode(const Key& key, Node* n) const;

    Node* FindGreaterOrEqual(const Key& key) const;

    Node* FindLessThan(const Key& key, Node** prev = nullptr) const;

    Node* FindLessThan(const Key& key, Node** prev, Node* root,
                       uint32_t top_level, uint32_t bottom_level) const;

    Node* FindLast() const;

    uint32_t Random() const;

    bool NeedHightByRandom(float probability) const;

    void FindSpliceForLevel(const Key& key, Node* before, Node* after,
                            uint32_t level, Node** out_prev, Node** out_next);

    void RecomputeSpliceLevels(const Key& key, Splice* splice,
                               uint32_t recompute_level);

    void FindSpliceForLevel(Node* target, Node* before, Node* after,
                            uint32_t level, Node** out_prev, Node** out_next);

    void RecomputeSpliceLevels(Node* target, Splice* splice,
                               uint32_t recompute_level);

   private:
    const uint16_t kMaxHeight_;
    const float probability_;

    Node* const head_;

    std::atomic_uint32_t max_height_;  // Height of the entire list
    Splice* seq_splice_;
    Alloc allocator_{};
};

template <typename Key, typename Value, typename Alloc>
struct SkipList<Key, Value, Alloc>::Splice {
    // The invariant of a Splice is that prev_[i+1].key <= prev_[i].key <
    // next_[i].key <= next_[i+1].key for all i.  That means that if a
    // key is bracketed by prev_[i] and next_[i] then it is bracketed by
    // all higher levels.  It is _not_ required that prev_[i]->Next(i) ==
    // next_[i] (it probably did at some point in the past, but intervening
    // or concurrent operations might have inserted nodes in between).
    uint32_t height_ = 0;
    Node** prev_;
    Node** next_;
};

template <typename Key, typename Value, typename Alloc>
struct SkipList<Key, Value, Alloc>::Position {
    SkipList* list_;
    Splice* splice_;

    explicit Position(SkipList* list) : list_(list), splice_(nullptr){};
    ~Position() {
        if (splice_ != nullptr) {
            list_->DeallocateSplice(splice_);
        }
    }
};

template <typename Key, typename Value, typename Alloc>
struct SkipList<Key, Value, Alloc>::Node {
    // Stores the height of the node in the memory location normally used for
    // next_[0].  This is used for passing data from AllocateKey to Insert.
    void SetHeight(const uint32_t height) {
        assert(sizeof(uint32_t) <= sizeof(next_[0]));
        memcpy(HeightPos(), &height, sizeof(uint32_t));
    }

    uint32_t height() {
        uint32_t rv;
        memcpy(&rv, HeightPos(), sizeof(uint32_t));
        return rv;
    }

    inline const Key* key() const {
        return reinterpret_cast<const Key*>(&next_[1]);
    }
    inline Value* value() { return reinterpret_cast<Value*>(ValuePos()); }
    inline char* KeyPos() { return reinterpret_cast<char*>(&next_[1]); }
    inline char* ValuePos() {
        return reinterpret_cast<char*>(&next_[1]) + sizeof(Key);
    }
    inline char* HeightPos() { return ValuePos() + sizeof(Value); }

    Node* Next(uint32_t level) {
        assert(level >= 0);
        return ((&next_[0] - level)->load(std::memory_order_acquire));
    }

    void SetNext(int level, Node* x) {
        assert(level >= 0);
        next_[0 - level].store(x, std::memory_order_release);
    }

    // Insert node after prev on specific level.
    void InsertAfter(Node* prev, int level) {
        SetNext(level, prev->Next(level));
        prev->SetNext(level, this);
    }

   private:
    // next_[0] is the lowest level link (level 0).  Higher levels are
    // stored _earlier_, so level 1 is at next_[-1].
    std::atomic<Node*> next_[1];
};

template <typename Key, typename Value, typename Alloc>
inline SkipList<Key, Value, Alloc>::Iterator::Iterator(SkipList* list) {
    SetList(list);
}

template <typename Key, typename Value, typename Alloc>
inline void SkipList<Key, Value, Alloc>::Iterator::SetList(SkipList* list) {
    list_ = list;
    SeekToFirst();
}

template <typename Key, typename Value, typename Alloc>
inline bool SkipList<Key, Value, Alloc>::Iterator::Valid() const {
    return node_ != nullptr;
}

template <typename Key, typename Value, typename Alloc>
inline const Key* SkipList<Key, Value, Alloc>::Iterator::key() const {
    assert(Valid());
    return node_->key();
}
template <typename Key, typename Value, typename Alloc>
inline Value* SkipList<Key, Value, Alloc>::Iterator::value() {
    assert(Valid());
    return node_->value();
}

template <typename Key, typename Value, typename Alloc>
inline bool SkipList<Key, Value, Alloc>::Iterator::Next() {
    assert(Valid());
    node_ = node_->Next(0);
    if (node_) {
        return true;
    }
    return false;
}

template <typename Key, typename Value, typename Alloc>
inline bool SkipList<Key, Value, Alloc>::Iterator::Prev() {
    // Instead of using explicit "prev" links, we just search for the
    // last node that falls before key.
    assert(Valid());
    node_ = list_->FindLessThan(*node_->key());
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
    if (node_) {
        return true;
    }
    return false;
}

template <typename Key, typename Value, typename Alloc>
inline bool SkipList<Key, Value, Alloc>::Iterator::Delete() {
    assert(Valid());
    auto tmp = node_->Next(0);
    auto r = list_->Delete(node_, list_->seq_splice_, true);
    node_ = tmp;
    if (node_ && r) {
        return true;
    }
    return false;
}

template <typename Key, typename Value, typename Alloc>
inline void SkipList<Key, Value, Alloc>::Iterator::Seek(const Key& target) {
    node_ = list_->FindGreaterOrEqual(target);
}

template <typename Key, typename Value, typename Alloc>
inline void SkipList<Key, Value, Alloc>::Iterator::SeekForPrev(
    const Key& target) {
    Seek(target);
    if (!Valid()) {
        SeekToLast();
    }
    while (Valid() && target < *key()) {
        Prev();
    }
}

template <typename Key, typename Value, typename Alloc>
inline void SkipList<Key, Value, Alloc>::Iterator::SeekToFirst() {
    node_ = list_->head_->Next(0);
}

template <typename Key, typename Value, typename Alloc>
inline void SkipList<Key, Value, Alloc>::Iterator::SeekToLast() {
    node_ = list_->FindLast();
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <typename Key, typename Value, typename Alloc>
inline auto SkipList<Key, Value, Alloc>::GetIterator() {
    Iterator it(this);
    return it;
}
template <typename Key, typename Value, typename Alloc>
uint32_t SkipList<Key, Value, Alloc>::Random() const {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<uint32_t> dis(0, 10000);
    return dis(gen);
}
template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::NeedHightByRandom(float probability) const {
    auto rd = Random();
    return rd < probability * 10000;
}

template <typename Key, typename Value, typename Alloc>
uint16_t SkipList<Key, Value, Alloc>::RandomHeight() {
    // Increase height with probability 1 in kBranching
    int height = 1;
    while (height < kMaxHeight_ && height < kMaxPossibleHeight &&
           NeedHightByRandom(probability_)) {
        height++;
    }
    assert(height > 0);
    assert(height <= kMaxHeight_);
    assert(height <= kMaxPossibleHeight);
    return height;
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::KeyIsAfterNode(const Key& key,
                                                 Node* n) const {
    // nullptr n is considered infinite
    assert(n != head_);
    return (n != nullptr) && (*n->key() < key);
}

template <typename Key, typename Value, typename Alloc>
typename SkipList<Key, Value, Alloc>::Node*
SkipList<Key, Value, Alloc>::FindGreaterOrEqual(const Key& key) const {
    // Note: It looks like we could reduce duplication by implementing
    // this function as FindLessThan(key)->Next(0), but we wouldn't be able
    // to exit early on equality and the result wouldn't even be correct.
    // A concurrent insert might occur after FindLessThan(key) but before
    // we get a chance to call Next(0).
    Node* x = head_;
    uint32_t level = GetMaxHeight() - 1;
    Node* last_bigger = nullptr;
    while (true) {
        Node* next = x->Next(level);
        if (next != nullptr) {
            PREFETCH(next->Next(level), 0, 1);
        }
        // Make sure the lists are sorted
        assert(x == head_ || next == nullptr ||
               KeyIsAfterNode(*next->key(), x));
        // Make sure we haven't overshot during our search
        assert(x == head_ || KeyIsAfterNode(key, x));
        int cmp =
            (next == nullptr || next == last_bigger)
                ? 1
                : (*next->key() == key ? 0 : (*next->key() < key ? -1 : 1));
        if (cmp == 0 || (cmp > 0 && level == 0)) {
            return next;
        } else if (cmp < 0) {
            // Keep searching in this list
            x = next;
        } else {
            // Switch to next list, reuse compare_() result
            last_bigger = next;
            level--;
        }
    }
}

template <typename Key, typename Value, typename Alloc>
typename SkipList<Key, Value, Alloc>::Node*
SkipList<Key, Value, Alloc>::FindLessThan(const Key& key, Node** prev) const {
    return FindLessThan(key, prev, head_, GetMaxHeight(), 0);
}

template <typename Key, typename Value, typename Alloc>
typename SkipList<Key, Value, Alloc>::Node*
SkipList<Key, Value, Alloc>::FindLessThan(const Key& key, Node** prev,
                                          Node* root, uint32_t top_level,
                                          uint32_t bottom_level) const {
    assert(top_level > bottom_level);
    int level = top_level - 1;
    Node* x = root;
    // KeyIsAfter(key, last_not_after) is definitely false
    Node* last_not_after = nullptr;
    while (true) {
        assert(x != nullptr);
        Node* next = x->Next(level);
        if (next != nullptr) {
            PREFETCH(next->Next(level), 0, 1);
        }
        assert(x == head_ || next == nullptr ||
               KeyIsAfterNode(*next->key(), x));
        assert(x == head_ || KeyIsAfterNode(key, x));
        if (next != last_not_after && KeyIsAfterNode(key, next)) {
            // Keep searching in this list
            assert(next != nullptr);
            x = next;
        } else {
            if (prev != nullptr) {
                prev[level] = x;
            }
            if (level == bottom_level) {
                return x;
            } else {
                // Switch to next list, reuse KeyIsAfterNode() result
                last_not_after = next;
                level--;
            }
        }
    }
}

template <typename Key, typename Value, typename Alloc>
typename SkipList<Key, Value, Alloc>::Node*
SkipList<Key, Value, Alloc>::FindLast() const {
    Node* x = head_;
    uint32_t level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr) {
            if (level == 0) {
                return x;
            } else {
                // Switch to next list
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <typename Key, typename Value, typename Alloc>
SkipList<Key, Value, Alloc>::SkipList(uint32_t max_height, float probability)
    : kMaxHeight_(static_cast<uint16_t>(max_height)),
      probability_(probability),
      head_(AllocateNode(max_height)),
      max_height_(1),
      seq_splice_(AllocateSplice()) {
    assert(max_height > 0 && kMaxHeight_ == static_cast<uint32_t>(max_height));
    assert(probability_ < 1 && probability_ > 0);
    if (head_ == nullptr) {
        throw std::runtime_error("skip list init failed because of OOM");
    }

    for (uint32_t i = 0; i < kMaxHeight_; ++i) {
        head_->SetNext(i, nullptr);
    }
}

template <typename Key, typename Value, typename Alloc>
SkipList<Key, Value, Alloc>::~SkipList() {
    DeallocateSplice(seq_splice_);
    auto next = head_->Next(0);
    DeallocateData<false>(head_);
    while (next != nullptr) {
        auto tmp = next->Next(0);
        DeallocateData(next);
        next = tmp;
    }
}

template <typename Key, typename Value, typename Alloc>
typename SkipList<Key, Value, Alloc>::Node*
SkipList<Key, Value, Alloc>::AllocateData(const Key& key, const Value& value) {
    constexpr auto key_size = sizeof(Key);
    constexpr auto value_size = sizeof(Value);
    typename SkipList<Key, Value, Alloc>::Node* node =
        AllocateNode(RandomHeight());
    if (node == nullptr) {
        return nullptr;
    }
    if constexpr (std::is_trivially_copyable<Key>::value) {
        memcpy(node->KeyPos(), &key, key_size);
    } else {
        new (node->KeyPos()) Key(key);
    }

    if constexpr (std::is_trivially_copyable<Value>::value) {
        memcpy(node->ValuePos(), &value, value_size);
    } else {
        new (node->ValuePos()) Value(value);
    }
    return node;
}

template <typename Key, typename Value, typename Alloc>
typename SkipList<Key, Value, Alloc>::Node*
SkipList<Key, Value, Alloc>::AllocateNode(uint32_t height) {
    constexpr auto kv_size = sizeof(Key) + sizeof(Value);

    const auto prefix = sizeof(std::atomic<Node*>) * (height - 1);

    // prefix is space for the height - 1 pointers that we store before
    // the Node instance (next_[-(height - 1) .. -1]).  Node starts at
    // raw + prefix, and holds the bottom-mode (level 0) skip list pointer
    // next_[0].  kv_size is the bytes for the key and value, which comes just
    // after the Node, sizeof(uint32_t) is the bytes for the hight.
    const auto data_size = prefix + sizeof(Node) + kv_size + sizeof(uint32_t);
    char* raw;
    try {
        raw = allocator_.allocate(data_size);
    } catch (std::bad_alloc&) {
        return nullptr;
    }
    Node* x = reinterpret_cast<Node*>(raw + prefix);
    x->SetHeight(height);
    return x;
}

template <typename Key, typename Value, typename Alloc>
typename SkipList<Key, Value, Alloc>::Splice*
SkipList<Key, Value, Alloc>::AllocateSplice() {
    // size of prev_ and next_
    const size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
    const size_t data_size = sizeof(Splice) + array_size * 2;
    char* raw;
    try {
        raw = allocator_.allocate(data_size);
    } catch (std::bad_alloc&) {
        return nullptr;
    }
    Splice* splice = reinterpret_cast<Splice*>(raw);
    splice->height_ = 0;
    splice->prev_ = reinterpret_cast<Node**>(raw + sizeof(Splice));
    splice->next_ = reinterpret_cast<Node**>(raw + sizeof(Splice) + array_size);
    return splice;
}

template <typename Key, typename Value, typename Alloc>
template <bool NeedDestruct>
void SkipList<Key, Value, Alloc>::DeallocateData(Node* node) {
    assert(node != nullptr);
    if constexpr (NeedDestruct) {
        if constexpr (std::is_destructible_v<Key>) {
            Key* key_p = reinterpret_cast<Key*>(node->KeyPos());
            key_p->~Key();
        }
        if constexpr (std::is_destructible_v<Value>) {
            Value* value_p = reinterpret_cast<Value*>(node->ValuePos());
            value_p->~Value();
        }
    }

    auto height = node->height();
    auto prefix = sizeof(Node*) * (height - 1);
    constexpr auto kv_size = sizeof(Key) + sizeof(Value);
    const auto data_size = prefix + sizeof(Node) + kv_size + sizeof(uint32_t);
    char* raw = reinterpret_cast<char*>(node) - prefix;
    allocator_.deallocate(raw, data_size);
}

template <typename Key, typename Value, typename Alloc>
void SkipList<Key, Value, Alloc>::DeallocateSplice(void* splice) {
    assert(splice != nullptr);
    const size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
    const size_t data_size = sizeof(Splice) + array_size * 2;
    char* buffer = reinterpret_cast<char*>(splice);
    allocator_.deallocate(buffer, data_size);
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::Insert(const Key& key, const Value& value) {
    return Insert(key, value, seq_splice_, false);
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::InsertBySequence(const Key& key,
                                                   const Value& value,
                                                   Position& pos) {
    return InsertWithHint(key, value, &pos.splice_);
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::InsertWithHint(const Key& key,
                                                 const Value& value,
                                                 Splice** hint) {
    assert(hint != nullptr);
    Splice* splice = *hint;
    if (splice == nullptr) {
        splice = AllocateSplice();
        if (splice == nullptr) {
            return false;
        }
        *hint = splice;
    }
    assert(splice != nullptr || splice->height_ >= 0);
    return Insert(key, value, splice, true);
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::Delete(const Key& key) {
    auto node = FindGreaterOrEqual(key);
    if (node == nullptr || !(*node->key() == key)) {
        return true;
    }
    return Delete(node, seq_splice_, false);
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::DeleteBySequence(const Key& key,
                                                   Position& pos) {
    return DeleteWithHint(key, &pos.splice_);
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::DeleteWithHint(const Key key, Splice** hint) {
    assert(hint != nullptr);
    Splice* splice = *hint;
    if (splice == nullptr) {
        splice = AllocateSplice();
        if (splice == nullptr) {
            return false;
        }
        *hint = splice;
    }
    assert(splice != nullptr || splice->height_ >= 0);
    auto node = FindGreaterOrEqual(key);
    if (node == nullptr || !(*node->key() == key)) {
        return true;
    }
    return Delete(node, seq_splice_, false);
}

template <typename Key, typename Value, typename Alloc>
void SkipList<Key, Value, Alloc>::FindSpliceForLevel(const Key& key,
                                                     Node* before, Node* after,
                                                     uint32_t level,
                                                     Node** out_prev,
                                                     Node** out_next) {
    while (true) {
        Node* next = before->Next(level);
        if (next != nullptr) {
            PREFETCH(next->Next(level), 0, 1);
            if (level > 0) {
                PREFETCH(next->Next(level - 1), 0, 1);
            }
        }
        assert(before == head_ || next == nullptr ||
               KeyIsAfterNode(*next->key(), before));
        if (next == after || !KeyIsAfterNode(key, next)) {
            // found it
            *out_prev = before;
            *out_next = next;
            return;
        }
        before = next;
    }
}

template <typename Key, typename Value, typename Alloc>
void SkipList<Key, Value, Alloc>::RecomputeSpliceLevels(
    const Key& key, Splice* splice, uint32_t recompute_level) {
    assert(recompute_level > 0);
    assert(recompute_level <= splice->height_);
    for (uint32_t i = recompute_level; i > 0; --i) {
        FindSpliceForLevel(key, splice->prev_[i], splice->next_[i], i - 1,
                           &splice->prev_[i - 1], &splice->next_[i - 1]);
    }
}

template <typename Key, typename Value, typename Alloc>
void SkipList<Key, Value, Alloc>::FindSpliceForLevel(Node* target, Node* before,
                                                     Node* after,
                                                     uint32_t level,
                                                     Node** out_prev,
                                                     Node** out_next) {
    while (true) {
        Node* next = before->Next(level);
        if (next != nullptr) {
            PREFETCH(next->Next(level), 0, 1);
            if (level > 0) {
                PREFETCH(next->Next(level - 1), 0, 1);
            }
        }
        assert(before == head_ || next == nullptr ||
               KeyIsAfterNode(*next->key(), before));
        if (next == target || next == after ||
            !KeyIsAfterNode(*target->key(), next)) {
            // found it
            *out_prev = before;
            *out_next = next;
            return;
        }
        before = next;
    }
}

template <typename Key, typename Value, typename Alloc>
void SkipList<Key, Value, Alloc>::RecomputeSpliceLevels(
    Node* target, Splice* splice, uint32_t recompute_level) {
    assert(recompute_level > 0);
    assert(recompute_level <= splice->height_);
    for (uint32_t i = recompute_level; i > 0; --i) {
        FindSpliceForLevel(target, splice->prev_[i], splice->next_[i], i - 1,
                           &splice->prev_[i - 1], &splice->next_[i - 1]);
    }
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::Insert(const Key& key, const Value& value,
                                         Splice* splice,
                                         bool allow_partial_splice_fix) {
    auto x = AllocateData(key, value);
    if (x == nullptr) {
        return false;
    }

    uint32_t height = x->height();
    assert(height >= 1 && height <= kMaxHeight_);

    uint32_t max_height = max_height_.load(std::memory_order_relaxed);
    while (height > max_height) {
        if (max_height_.compare_exchange_weak(max_height, height)) {
            // successfully updated it
            max_height = height;
            break;
        }
        // else retry, possibly exiting the loop because somebody else
        // increased it
    }
    assert(max_height <= kMaxPossibleHeight);

    uint32_t recompute_height = 0;
    if (splice->height_ < max_height_) {
        // Either splice has never been used or max_height has grown since
        // last use.  We could potentially fix it in the latter case, but
        // that is tricky.
        splice->prev_[max_height_] = head_;
        splice->next_[max_height_] = nullptr;
        splice->height_ = max_height_;
        recompute_height = max_height_;
    } else {
        // Splice is a valid proper-height splice that brackets some
        // key, but does it bracket this one?  We need to validate it and
        // recompute a portion of the splice (levels 0..recompute_height-1)
        // that is a superset of all levels that don't bracket the new key.
        // Several choices are reasonable, because we have to balance the work
        // saved against the extra comparisons required to validate the Splice.
        //
        // One strategy is just to recompute all of orig_splice_height if the
        // bottom level isn't bracketing.  This pessimistically assumes that
        // we will either get a perfect Splice hit (increasing sequential
        // inserts) or have no locality.
        //
        // Another strategy is to walk up the Splice's levels until we find
        // a level that brackets the key.  This strategy lets the Splice
        // hint help for other cases: it turns insertion from O(log N) into
        // O(log D), where D is the number of nodes in between the key that
        // produced the Splice and the current insert (insertion is aided
        // whether the new key is before or after the splice).  If you have
        // a way of using a prefix of the key to map directly to the closest
        // Splice out of O(sqrt(N)) Splices and we make it so that splices
        // can also be used as hints during read, then we end up with Oshman's
        // and Shavit's SkipTrie, which has O(log log N) lookup and insertion
        // (compare to O(log N) for skip list).
        //
        // We control the pessimistic strategy with allow_partial_splice_fix.
        // A good strategy is probably to be pessimistic for seq_splice_,
        // optimistic if the caller actually went to the work of providing
        // a Splice.
        while (recompute_height < max_height_) {
            if (splice->prev_[recompute_height]->Next(recompute_height) !=
                splice->next_[recompute_height]) {
                // splice isn't tight at this level, there must have been some
                // inserts to this location that didn't update the splice.  We
                // might only be a little stale, but if the splice is very stale
                // it would be O(N) to fix it.  We haven't used up any of our
                // budget of comparisons, so always move up even if we are
                // pessimistic about
                // our chances of success.
                ++recompute_height;
            } else if (splice->prev_[recompute_height] != head_ &&
                       !KeyIsAfterNode(key, splice->prev_[recompute_height])) {
                // key is from before splice
                if (allow_partial_splice_fix) {
                    // skip all levels with the same node without more
                    // comparisons
                    Node* bad = splice->prev_[recompute_height];
                    while (splice->prev_[recompute_height] == bad) {
                        ++recompute_height;
                    }
                } else {
                    // we're pessimistic, recompute everything
                    recompute_height = max_height_;
                }
            } else if (KeyIsAfterNode(key, splice->next_[recompute_height])) {
                // key is from after splice
                if (allow_partial_splice_fix) {
                    Node* bad = splice->next_[recompute_height];
                    while (splice->next_[recompute_height] == bad) {
                        ++recompute_height;
                    }
                } else {
                    recompute_height = max_height_;
                }
            } else {
                // this level brackets the key, we won!
                break;
            }
        }
    }
    assert(recompute_height <= max_height_);
    if (recompute_height > 0) {
        RecomputeSpliceLevels(key, splice, recompute_height);
    }

    bool splice_is_valid = true;
    for (uint32_t i = 0; i < height; ++i) {
        if (i >= recompute_height &&
            splice->prev_[i]->Next(i) != splice->next_[i]) {
            FindSpliceForLevel(key, splice->prev_[i], nullptr, i,
                               &splice->prev_[i], &splice->next_[i]);
        }
        // Checking for duplicate keys on the level 0 is sufficient
        if (i == 0 && splice->next_[i] != nullptr &&
            (*(splice->next_[i]->key()) < *(x->key()) ||
             *(x->key()) == *(splice->next_[i]->key()))) [[unlikely]] {
            // duplicate key
            DeallocateData(x);
            return false;
        }
        if (i == 0 && splice->prev_[i] != head_ &&
            (*(x->key()) < *(splice->prev_[i]->key()) ||
             *(splice->prev_[i]->key()) == *(x->key()))) [[unlikely]] {
            // duplicate key
            DeallocateData(x);
            return false;
        }
        assert(splice->next_[i] == nullptr ||
               *x->key() < *(splice->next_[i]->key()));
        assert(splice->prev_[i] == head_ ||
               *(splice->prev_[i]->key()) < *x->key());
        assert(splice->prev_[i]->Next(i) == splice->next_[i]);
        x->SetNext(i, splice->next_[i]);
        splice->prev_[i]->SetNext(i, x);
    }
    if (splice_is_valid) {
        for (uint32_t i = 0; i < height; ++i) {
            splice->prev_[i] = x;
        }
        assert(splice->prev_[splice->height_] == head_);
        assert(splice->next_[splice->height_] == nullptr);
        for (uint32_t i = 0; i < splice->height_; ++i) {
            assert(splice->next_[i] == nullptr ||
                   key < *(splice->next_[i]->key()));
            assert(splice->prev_[i] == head_ ||
                   *(splice->prev_[i]->key()) < key ||
                   *(splice->prev_[i]->key()) == key);
            assert(splice->prev_[i + 1] == splice->prev_[i] ||
                   splice->prev_[i + 1] == head_ ||
                   *(splice->prev_[i + 1]->key()) < *(splice->prev_[i]->key()));
            assert(splice->next_[i + 1] == splice->next_[i] ||
                   splice->next_[i + 1] == nullptr ||
                   *(splice->next_[i]->key()) < *(splice->next_[i + 1]->key()));
        }
    } else {
        splice->height_ = 0;
    }
    return true;
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::Delete(Node* target_node, Splice* splice,
                                         bool allow_partial_splice_fix) {
    uint32_t recompute_height = 0;
    if (splice->height_ < max_height_) {
        splice->prev_[max_height_] = head_;
        splice->next_[max_height_] = nullptr;
        splice->height_ = max_height_;
        recompute_height = max_height_;
    } else {
        while (recompute_height < max_height_) {
            if (splice->prev_[recompute_height]->Next(recompute_height) !=
                splice->next_[recompute_height]) {
                ++recompute_height;
            } else if (splice->prev_[recompute_height] != head_ &&
                       !KeyIsAfterNode(*target_node->key(),
                                       splice->prev_[recompute_height])) {
                // key is from before splice
                if (allow_partial_splice_fix) {
                    // skip all levels with the same node without more
                    // comparisons
                    Node* bad = splice->prev_[recompute_height];
                    while (splice->prev_[recompute_height] == bad) {
                        ++recompute_height;
                    }
                } else {
                    // we're pessimistic, recompute everything
                    recompute_height = max_height_;
                }
            } else if (KeyIsAfterNode(*target_node->key(),
                                      splice->next_[recompute_height])) {
                // key is from after splice
                if (allow_partial_splice_fix) {
                    Node* bad = splice->next_[recompute_height];
                    while (splice->next_[recompute_height] == bad) {
                        ++recompute_height;
                    }
                } else {
                    recompute_height = max_height_;
                }
            } else {
                // this level brackets the key, we won!
                break;
            }
        }
    }
    assert(recompute_height <= max_height_);
    if (recompute_height > 0) {
        RecomputeSpliceLevels(target_node, splice, recompute_height);
    }
    for (uint32_t i = 0; i < max_height_; ++i) {
        if (i >= recompute_height &&
            splice->prev_[i]->Next(i) != splice->next_[i]) {
            FindSpliceForLevel(target_node, splice->prev_[i], nullptr, i,
                               &splice->prev_[i], &splice->next_[i]);
        }
        if (splice->next_[i] != nullptr && splice->next_[i] == target_node) {
            splice->prev_[i]->SetNext(i, splice->next_[i]->Next(i));
            splice->next_[i] = splice->next_[i]->Next(i);
        }
    }
    DeallocateData(target_node);
    return true;
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::Contains(const Key& key) const {
    Node* x = FindGreaterOrEqual(key);
    if (x != nullptr && key == *x->key()) {
        return true;
    } else {
        return false;
    }
}

template <typename Key, typename Value, typename Alloc>
bool SkipList<Key, Value, Alloc>::Find(const Key& key, Value** value) {
    Node* x = FindGreaterOrEqual(key);
    if (x != nullptr && key == *x->key()) {
        *value = x->value();
        return true;
    } else {
        return false;
    }
}

}  // namespace table

#endif
