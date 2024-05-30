#include "inlineskiplist.h"

#include <future>

#include "gtest/gtest.h"

struct Key {
    int i;
};
bool operator<(const Key& l, const Key& r) { return l.i < r.i; }

bool operator==(const Key& l, const Key& r) { return l.i == r.i; }

struct Value {
    Value(int i) : i(i) { v = new int[10]; }
    ~Value() { delete[] v; }

    Value(const Value& o) {
        i = o.i;
        v = new int[10];
        memcpy(v, o.v, sizeof(int) * 10);
    }

    int i;
    int* v;
};

using namespace table;
TEST(InlineSkipList, insert) {
    SkipList<int, int> intlist;
    EXPECT_EQ(true, intlist.Insert(1, 1));
    EXPECT_EQ(true, intlist.Insert(2, 2));
    EXPECT_EQ(true, intlist.Insert(3, 3));
    EXPECT_EQ(true, intlist.Insert(4, 4));
    EXPECT_EQ(false, intlist.Insert(4, 4));
    EXPECT_EQ(true, intlist.Insert(5, 5));

    SkipList<float, float> floatlist;
    EXPECT_EQ(true, floatlist.Insert(1.0, 1));
    EXPECT_EQ(true, floatlist.Insert(2.2, 2));
    EXPECT_EQ(true, floatlist.Insert(3.3, 3));
    EXPECT_EQ(true, floatlist.Insert(4.5, 4));
    EXPECT_EQ(false, floatlist.Insert(4.5, 48));
    EXPECT_EQ(true, floatlist.Insert(5.9, 5));

    SkipList<Key, Value> objectlist;
    EXPECT_EQ(true, objectlist.Insert({1}, {2}));
    EXPECT_EQ(true, objectlist.Insert({2}, {2}));
    EXPECT_EQ(true, objectlist.Insert({3}, {3}));
    EXPECT_EQ(true, objectlist.Insert({4}, {4}));
    EXPECT_EQ(false, objectlist.Insert({4}, {4}));
    EXPECT_EQ(true, objectlist.Insert({5}, {5}));
}

TEST(InlineSkipList, insertAndFind) {
    SkipList<Key, Value> list;
    constexpr int count = 100000;
    for (int i = 0; i < count; ++i) {
        auto r = list.Insert({i}, {i});
        EXPECT_EQ(true, r);
    }

    SkipList<Key, Value>::Position pos(&list);
    for (int i = count; i < count * 2; ++i) {
        list.InsertBySequence({i}, {i}, pos);
    }
    for (int i = 0; i < count * 2; ++i) {
        Value* value;
        auto r = list.Find({i}, &value);
        ASSERT_EQ(true, r);
        EXPECT_EQ(i, value->i);
    }
}

TEST(InlineSkipList, Delete) {
    SkipList<Key, Value> list;
    constexpr int count = 100000;
    SkipList<Key, Value>::Position pos(&list);
    for (int i = 0; i < count; ++i) {
        list.InsertBySequence({i}, {i}, pos);
    }
    for (int i = 0; i < count; ++i) {
        if (i % 2 == 0) {
            auto r = list.Delete({i});
            EXPECT_EQ(true, r);
        }
    }
    for (int i = 0; i < count; ++i) {
        if (i % 2 == 0) {
            auto r = list.Contains({i});
            EXPECT_EQ(false, r);
        } else {
            Value* value;
            auto r = list.Find({i}, &value);
            ASSERT_EQ(true, r);
            EXPECT_EQ(i, value->i);
        }
    }
}

TEST(InlineSkipList, Iterator) {
    SkipList<Key, Value> list;
    constexpr int count = 100000;
    SkipList<Key, Value>::Position pos(&list);
    for (int i = 0; i < count; ++i) {
        auto r = list.InsertBySequence({i}, {i}, pos);
        EXPECT_EQ(true, r);
    }
    auto it = list.GetIterator();
    EXPECT_EQ(0, it.key()->i);
    EXPECT_EQ(0, it.value()->i);
    int i = 0;
    while (it.Next()) {
        i++;
        EXPECT_EQ(i, it.key()->i);
        EXPECT_EQ(i, it.value()->i);
    }

    it.SeekToLast();
    EXPECT_EQ(count - 1, it.key()->i);
    EXPECT_EQ(count - 1, it.value()->i);

    while (it.Prev()) {
        i--;
        EXPECT_EQ(i, it.key()->i);
        EXPECT_EQ(i, it.value()->i);
    }

    it.Seek({count / 2});
    EXPECT_EQ(count / 2, it.key()->i);
    EXPECT_EQ(count / 2, it.value()->i);
    it.SeekForPrev({count / 2});
    EXPECT_EQ(count / 2, it.key()->i);
    EXPECT_EQ(count / 2, it.value()->i);

    it.Delete();
    it.SeekForPrev({count / 2});
    EXPECT_EQ(count / 2 - 1, it.key()->i);
    EXPECT_EQ(count / 2 - 1, it.value()->i);

    while (it.Delete()) {
    }

    it.SeekToFirst();
    int maxkey;
    while (it.Next()) {
        maxkey = it.key()->i;
    }
    EXPECT_EQ(count / 2 - 2, maxkey);
}

TEST(InlineSkipList, concurrence) {
    constexpr int count = 100000;
    SkipList<Key, Value> list;
    auto fun = [&list](int start) {
        for (int i = start; i < count; i += 2) {
            auto r = list.Insert({i}, {i});
            EXPECT_EQ(true, r);
        }
    };

    auto future1 = std::async(fun, 0);
    auto future2 = std::async(fun, 1);

    future1.get();
    future2.get();

    for (int i = 0; i < count * 2; ++i) {
        Value* value;
        auto r = list.Find({i}, &value);
        ASSERT_EQ(true, r);
        EXPECT_EQ(i, value->i);
    }
}
