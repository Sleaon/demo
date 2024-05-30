#include <fmt/core.h>
#include <fmt/format.h>

#include <functional>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

#include "echo.pb.h"
#include "messaging_service.h"
#include "msg_addr.h"
#include "rpc_protocol_impl.h"
#include "rpc_verb.h"

using namespace seastar;
namespace bpo = boost::program_options;
using namespace std::chrono_literals;

seastar::logger lg("rpc_demo");

class RpcTest {
   public:
    future<echo::EchoResponse> Echo(const rpc::client_info& info,
                                    echo::EchoRequest msg) {
        lg.info("test6 client {}, msg {}, {}/{}\n",
                inet_ntoa(info.addr.as_posix_sockaddr_in().sin_addr),
                msg.message(), this_shard_id(), smp::count);
        co_await sleep(5s);
        echo::EchoResponse res;
        auto m = fmt::format("say {}", msg.message());
        res.set_message(m);
        co_return res;
    }

    auto EchoCall(int i) {
        echo::EchoRequest req;
        auto m = fmt::format("hello {}", i);
        req.set_message(m);
        return rpcserver::send_message<future<echo::EchoResponse>>(
                   &message_service_, rpcserver::RpcVerb::kRpcTest_Echo,
                   rpcserver::msg_addr(seastar::net::inet_address("127.0.0.1")),
                   std::move(req))
            .then_wrapped([](auto&& f) {
                try {
                    auto msg = f.get();
                    lg.info("client {} {}/{}", msg.message(), this_shard_id(),
                            smp::count);
                } catch (rpc::canceled_error&) {
                    lg.info("canceled {}/{}\n", this_shard_id(), smp::count);
                } catch (...) {
                    lg.info("wrong exception {}/{}/n", this_shard_id(),
                            smp::count);
                }
            });
    }

    explicit RpcTest(rpcserver::messaging_service& message_service)
        : message_service_(message_service) {
        message_service_;
        lg.info("rpctest construct, {}/{}", this_shard_id(), smp::count);
        rpcserver::register_handler(
            &message_service_, rpcserver::RpcVerb::kRpcTest_Echo,
            [this](const rpc::client_info& info, echo::EchoRequest msg) {
                return Echo(info, msg);
            });
    }

   private:
    rpcserver::messaging_service& message_service_;
};

int main(int ac, char** av) {
    app_template app;
    app.add_options()("port", bpo::value<uint16_t>()->default_value(10000),
                      "RPC server port")(
        "server", bpo::value<bool>()->default_value(true), "Server address");
    lg.info("start ");
    sharded<rpcserver::messaging_service> message_service;
    sharded<RpcTest> rpc_test;

    return app.run_deprecated(ac, av, [&] {
        engine().at_exit([&] -> future<> {
            co_await message_service.stop();
            co_await rpc_test.stop();
            co_return;
        });
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        message_service.start(std::string("test_host"),
                              net::inet_address("127.0.0.1"), port);
        rpc_test.start(std::ref(message_service));
        if (!config["server"].as<bool>()) {
            lg.info("client");

            for (auto i = 0; i < 100; i++) {
                lg.info("iteration={:d}\n", i);
                rpc_test.invoke_on_all(&RpcTest::EchoCall, i);
            }
        } else {
            message_service.invoke_on_all(
                &rpcserver::messaging_service::start_listen);
        }
    });
}
// #include <functional>
// #include <iostream>
// #include <tuple>
// #include <type_traits>
// #include <string>

// #include <cstddef> // for size_t
// #include <cstdint> // for intmm_t
// #include <type_traits>

// namespace ct
// {
// namespace details {
//     template <typename T, size_t numerator, size_t denominator>
//     struct float_at_t {
//         static_assert(false
//             || std::is_same<T, float>::value
//             || std::is_same<T, double>::value
//         , "");
//         static constexpr auto value = static_cast<T>(1) *
//             numerator / denominator;
//     };
// }   // namespace details
// template <typename T, T value_>
// struct enum_t {
//     static constexpr auto value = value_;
// };
// template <size_t numerator, size_t denominator>
// struct f32 : details::float_at_t<float, numerator, denominator> {};
// template <size_t numerator, size_t denominator>
// struct f64 : details::float_at_t<double, numerator, denominator> {};
// template <int8_t   value> struct i8  : enum_t<decltype(value), value> {};
// template <int16_t  value> struct i16 : enum_t<decltype(value), value> {};
// template <int32_t  value> struct i32 : enum_t<decltype(value), value> {};
// template <int64_t  value> struct i64 : enum_t<decltype(value), value> {};
// template <uint8_t  value> struct u8  : enum_t<decltype(value), value> {};
// template <uint16_t value> struct u16 : enum_t<decltype(value), value> {};
// template <uint32_t value> struct u32 : enum_t<decltype(value), value> {};
// template <uint64_t value> struct u64 : enum_t<decltype(value), value> {};

// template <typename... items>
// class map;
// namespace details {

// template <typename T>
// struct remove_cvref {
//     using type = typename std::remove_cv<
//                 typename std::remove_reference<T>::type
//             >::type;
// };

// template <typename T>
// struct is_cv_or_ref {
//     static constexpr auto value = !std::is_same<
//             T, typename remove_cvref<T>::type
//         >::value;
// };

// template <typename... args_t>
// struct constant_false {
//     static constexpr auto value = false;
// };

// template <typename T>
// struct has_member_value {
//   private:
//     template <typename U, int = sizeof(&U::value)>
//     static auto test(int) -> uint8_t;
//     template <typename U>
//     static auto test(long) -> uint16_t;
//   public:
//     static constexpr auto value = sizeof(test<T>(0)) == sizeof(uint8_t);
// };

// template <typename T, typename U>
// struct pair_t {
//     using first_t = T;
//     using second_t = U;
// };

// template <typename key_t, typename...>
// struct map_contains_t;
// template <typename key_t, typename item, typename... items>
// struct map_contains_t<key_t, item, items...> {
//     static constexpr auto value = false
//         || std::is_same<key_t, typename item::first_t>::value
//         || map_contains_t<key_t, items...>::value
//     ;
// };
// template <typename key_t>
// struct map_contains_t<key_t> {
//     static constexpr auto value = false;
// };

// template <typename key_t, typename... items>
// struct map_at_find_t;
// template <bool found, typename key_t, typename item, typename... items>
// struct map_at_find_t_ {
//     using type = typename map_at_find_t<key_t, items...>::type;
// };
// template <typename key_t, typename item, typename... items>
// struct map_at_find_t_<true, key_t, item, items...> {
//     using type = typename item::second_t;
// };
// template <typename key_t>
// struct map_at_find_t<key_t> {
//     static_assert(constant_false<key_t>::value, "no key");
// };
// template <typename key_t, typename item, typename... items>
// struct map_at_find_t<key_t, item, items...> {
//   private:
//     static constexpr auto found = std::is_same<
//             key_t, typename item::first_t
//         >::value;
//   public:
//     using type = typename map_at_find_t_<found, key_t, item, items...>::type;
// };

// template <typename T, bool has_value = has_member_value<T>::value>
// struct map_at_t_ {
//     using type = T;
// };
// template <typename T>
// struct map_at_t_<T, true>: map_at_t_<T, false> {
//     static constexpr auto value = T::value;
// };
// template <typename key_t, typename... items>
// struct map_at_t: map_at_t_<typename map_at_find_t<key_t, items...>::type> {};

// template <typename key_t, typename value_t, typename... items>
// struct map_insert_t {
//     static_assert(!details::is_cv_or_ref<key_t>::value, "");
//     static_assert(!details::is_cv_or_ref<value_t>::value, "");
//     static_assert(!::ct::map<items...>::template contains<key_t>::value, "");
//     using type = ::ct::map<items..., details::pair_t<key_t, value_t>>;
// };

// template <typename key_t, typename M, typename... items>
// struct map_erase_t;
// template <bool found, typename key_t, typename M, typename item,
//                 typename... items>
// struct map_erase_t_;
// template <typename key_t, typename... items1, typename item, typename...
// items2> struct map_erase_t_<false, key_t, ::ct::map<items1...>, item,
// items2...> {
//     using type = typename map_erase_t<
//             key_t, ::ct::map<items1..., item>, items2...
//         >::type;
// };
// template <typename key_t, typename... items1, typename item, typename...
// items2> struct map_erase_t_<true, key_t, ::ct::map<items1...>, item,
// items2...> {
//     using type = ::ct::map<items1..., items2...>;
// };
// template <typename key_t, typename M>
// struct map_erase_t<key_t, M> {
//     static_assert(constant_false<key_t>::value, "no key");
// };
// template <typename key_t, typename... items1, typename item, typename...
// items2> struct map_erase_t<key_t, ::ct::map<items1...>, item, items2...> {
//     using type = typename map_erase_t_<
//             std::is_same<key_t, typename item::first_t>::value,
//             key_t, ::ct::map<items1...>, item, items2...
//         >::type;
// };

// template <typename M, typename... items>
// struct map_merge_t_;
// template <typename M>
// struct map_merge_t_<M> {
//     using type = M;
// };
// template <typename... items1, typename item, typename... items2>
// struct map_merge_t_<::ct::map<items1...>, item, items2...> {
//     static_assert(
//         !::ct::map<items1...>::template contains<typename
//         item::first_t>::value, "the same key exists"
//     );
//     using type = typename map_merge_t_<
//             ::ct::map<items1..., item>, items2...
//         >::type;
// };
// template <typename M1, typename M2>
// struct map_merge_t;
// template <typename M, typename... items>
// struct map_merge_t<M, ::ct::map<items...>> {
//     using type = typename map_merge_t_<M, items...>::type;
// };

// template <bool len_eq, typename M, typename... items>
// struct map_equal_t_ {
//     static constexpr auto value = false;
// };
// template <typename M>
// struct map_equal_t_<true, M> {
//     static constexpr auto value = true;
// };
// template <typename M, typename item, typename... items>
// struct map_equal_t_<true, M, item, items...> {
//     static constexpr auto value = true
//         && M::template contains<typename item::first_t>::value
//         && map_equal_t_<true, M, items...>::value
//     ;
// };
// template <typename M1, typename M2>
// struct map_equal_t;
// template <typename M, typename... items>
// struct map_equal_t<M, ::ct::map<items...>> {
//     static constexpr auto value = map_equal_t_<
//             M::size == sizeof...(items), M, items...
//         >::value;
// };

// template <typename M, typename... args_t>
// struct map_from_list_t;
// template <typename M>
// struct map_from_list_t<M> {
//     using type = M;
// };
// template <typename M, typename rem>
// struct map_from_list_t<M, rem> {
//     static_assert(constant_false<rem>::value, "missing value");
// };
// template <typename M, typename key_t, typename value_t, typename... args_t>
// struct map_from_list_t<M, key_t, value_t, args_t...> {
//     static_assert(!M::template contains<key_t>::value, "");
//     using type = typename map_from_list_t<
//             typename M::template insert_t<key_t, value_t>, args_t...
//         >::type;
// };

// }   // namespace details

// template <typename... args_t>
// struct map_from_list: details::map_from_list_t<::ct::map<>, args_t...> {};
// template <typename... args_t>
// using map_from_list_t = typename map_from_list<args_t...>::type;

// template <typename... items>
// class map {
//   private:
//     using self_t = map<items...>;

//   public:
//     static constexpr auto size = sizeof...(items);
//     template <typename key_t>
//     struct contains: details::map_contains_t<key_t, items...> {};
//     template <typename M>
//     struct equal: details::map_equal_t<self_t, M> {};
//     template <typename key_t>
//     static constexpr auto contains_v = self_t::template
//     contains<key_t>::value; template <typename M> static constexpr auto
//     equal_v = self_t::template equal<M>::value;

//     template <typename key_t>
//     struct at: details::map_at_t<key_t, items...> {};
//     template <typename key_t>
//     using at_t = typename self_t::template at<key_t>::type;
//     template <typename key_t>
//     static constexpr auto at_v = self_t::template at<key_t>::value;

//     template <typename key_t, typename value_t>
//     struct insert: details::map_insert_t<key_t, value_t, items...> {};
//     template <typename key_t, typename value_t>
//     using insert_t = typename self_t::template insert<key_t, value_t>::type;

//     template <typename key_t>
//     struct erase: details::map_erase_t<key_t, map<>, items...> {};
//     template <typename key_t>
//     using erase_t = typename self_t::template erase<key_t>::type;

//     template <typename M>
//     struct merge: details::map_merge_t<self_t, M> {};
//     template <typename M>
//     using merge_t = typename self_t::template merge<M>::type;
// };  // class map

// } // namespace ct

// constexpr std::size_t ct_hash(const char* input) {
//     std::size_t result = 0;
//     while (*input) {
//         result ^= static_cast<std::size_t>(*input++) + 0x9e3779b9 + (result
//         << 6) + (result >> 2);
//     }
//     return result;
// }

// template <std::size_t N>
// struct Function{
//   static constexpr size_t i = N;
//   static void print(){
//     std::cout << "Function: " << i << std::endl;
//   }
// };
// template <std::size_t N>
// struct IntKey {
//     static constexpr std::size_t value = N;
// };

// using m1 = ct::map<>::insert_t<IntKey<1>, Function<1>>;

// struct Register{

// using m1 = ct::map<>::insert_t<IntKey<1>, Function<1>>;

// constexpr void a(std::string& name);

// };

// template<size_t id>
// struct RegisterHelper{
//   using Func = m1::insert_t<IntKey<id>, Function<id>>;
// };

// using m2 = m1::insert_t<IntKey<ct_hash("abc")>, Function<ct_hash("abc")>>;

// int main(){
//  using f1 = RegisterHelper<ct_hash("abc")>;
//  using f2 = RegisterHelper<ct_hash("tewr")>;
//  using f3 = RegisterHelper<ct_hash("abcv")>;
//  std::cout << "size: " << m1::size << std::endl;
// //  m1::at_t<IntKey<ct_hash("abc")>>::print();
// }
// #include <sys/types.h>

// #include <atomic>
// #include <cassert>
// #include <chrono>
// #include <cstdint>
// #include <cstdlib>
// #include <cstring>
// #include <iostream>
// #include <memory>
// #include <ostream>
// #include <random>
// #include <type_traits>

// #if defined(__aarch64__)
// //  __builtin_prefetch(..., 1) turns into a prefetch into prfm pldl3keep. On
// // arm64 we want this as close to the core as possible to turn it into a
// // L1 prefetech unless locality == 0 in which case it will be turned into a
// // non-temporal prefetch
// #define PREFETCH(addr, rw, locality) \
//   __builtin_prefetch(addr, rw, locality >= 1 ? 3 : locality)
// #else
// #define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)
// #endif

// template <typename T>
// concept ComparableAndCopyble =
//     !std::is_pointer_v<T> && std::copyable<T> && requires(T l, T r) {
//   l < r;
//   l == r;
// };

// template <typename T>
// concept Copyable = std::copyable<T>;

// template <ComparableAndCopyble Key, Copyable Value>
// class SkipList {
//   static const uint16_t kMaxPossibleHeight = 32;
//   struct Node;
//   struct Splice;

//  public:
//   struct Position;
//   explicit SkipList(uint32_t max_height = 12, float probability = 0.5);
//   SkipList(const SkipList&) = delete;
//   auto operator=(const SkipList&) = delete;
//   SkipList(SkipList&&) noexcept;
//   auto operator=(SkipList&&) noexcept;
//   ~SkipList();

//   bool Insert(const Key& key, const Value& value);
//   bool InsertBySequence(const Key& key, const Value& value, Position& pos);

//   bool Delete(const Key& key);
//   bool DeleteBySequence(const Key& key, Position& pos);

//   bool Contains(const Key& key) const;
//   bool Find(const Key& key, Value** value);

//   auto GetIterator();

//   class Iterator {
//    public:
//     explicit Iterator(SkipList* list);

//     void SetList(SkipList* list);

//     bool Valid() const;
//     const Key* key() const;
//     Value* value();
//     bool Next();
//     bool Prev();

//     bool Delete();

//     void Seek(const Key& target);

//     void SeekForPrev(const Key& target);

//     void SeekToFirst();

//     void SeekToLast();

//    private:
//     SkipList* list_;
//     Node* node_;
//   };

//  private:
//   bool Insert(const Key& key, const Value& value, Splice* splice,
//               bool allow_partial_splice_fix);
//   bool InsertWithHint(const Key& key, const Value& value, Splice** hint);
//   bool Delete(Node* target_node, Splice* splice, bool
//   allow_partial_splice_fix); bool DeleteWithHint(const Key key, Splice**
//   hint);

//   Node* AllocateData(const Key& key, const Value& value);
//   Node* AllocateNode(int height);
//   // Allocate a splice using allocator.
//   Splice* AllocateSplice();
//   void DeallocateData(Node* node);
//   void DeallocateSplice(void* splice);

//   inline int GetMaxHeight() const { return max_height_; }
//   uint16_t RandomHeight();
//   bool KeyIsAfterNode(const Key& key, Node* n) const;

//   Node* FindGreaterOrEqual(const Key& key) const;

//   Node* FindLessThan(const Key& key, Node** prev = nullptr) const;

//   Node* FindLessThan(const Key& key, Node** prev, Node* root, int top_level,
//                      int bottom_level) const;

//   Node* FindLast() const;

//   uint32_t Random() const;

//   bool NeedHightByRandom(float probability) const;

//   void FindSpliceForLevel(const Key& key, Node* before, Node* after, int
//   level,
//                           Node** out_prev, Node** out_next);

//   void RecomputeSpliceLevels(const Key& key, Splice* splice,
//                              int recompute_level);

//   void FindSpliceForLevel(Node* target, Node* before, Node* after, int level,
//                           Node** out_prev, Node** out_next);

//   void RecomputeSpliceLevels(Node* target, Splice* splice, int
//   recompute_level);

//  private:
//   const uint16_t kMaxHeight_;
//   const float probability_;

//   Node* const head_;

//   int max_height_;  // Height of the entire list
//   Splice* seq_splice_;
// };

// template <ComparableAndCopyble Key, Copyable Value>
// struct SkipList<Key, Value>::Splice {
//   // The invariant of a Splice is that prev_[i+1].key <= prev_[i].key <
//   // next_[i].key <= next_[i+1].key for all i.  That means that if a
//   // key is bracketed by prev_[i] and next_[i] then it is bracketed by
//   // all higher levels.  It is _not_ required that prev_[i]->Next(i) ==
//   // next_[i] (it probably did at some point in the past, but intervening
//   // or concurrent operations might have inserted nodes in between).
//   int height_ = 0;
//   Node** prev_;
//   Node** next_;
// };

// template <ComparableAndCopyble Key, Copyable Value>
// struct SkipList<Key, Value>::Position {
//   SkipList* list_;
//   Splice* splice_;

//   explicit Position(SkipList* list) : list_(list), splice_(nullptr){};
//   ~Position() {
//     if (splice_ != nullptr) {
//       list_->DeallocateSplice(splice_);
//     }
//   }
// };

// template <ComparableAndCopyble Key, Copyable Value>
// struct SkipList<Key, Value>::Node {
//   // Stores the height of the node in the memory location normally used for
//   // next_[0].  This is used for passing data from AllocateKey to Insert.
//   void SetHeight(const uint32_t height) {
//     assert(sizeof(uint32_t) <= sizeof(next_[0]));
//     memcpy(HeightPos(), &height, sizeof(uint32_t));
//   }

//   uint32_t height() {
//     uint32_t rv;
//     memcpy(&rv, HeightPos(), sizeof(uint32_t));
//     return rv;
//   }

//   const Key* key() const { return reinterpret_cast<const Key*>(&next_[1]); }
//   Value* value() { return reinterpret_cast<Value*>(ValuePos()); }
//   char* KeyPos() { return reinterpret_cast<char*>(&next_[1]); }
//   char* ValuePos() { return reinterpret_cast<char*>(&next_[1]) + sizeof(Key);
//   } char* HeightPos() { return ValuePos() + sizeof(Value); }

//   Node* Next(int level) {
//     assert(level >= 0);
//     return *(&next_[0] - level);
//   }

//   void SetNext(int level, Node* x) {
//     assert(level >= 0);
//     next_[0 - level] = x;
//   }

//   // Insert node after prev on specific level.
//   void InsertAfter(Node* prev, int level) {
//     SetNext(level, prev->Next(level));
//     prev->SetNext(level, this);
//   }

//  private:
//   // next_[0] is the lowest level link (level 0).  Higher levels are
//   // stored _earlier_, so level 1 is at next_[-1].
//   Node* next_[1];
// };

// template <ComparableAndCopyble Key, Copyable Value>
// inline SkipList<Key, Value>::Iterator::Iterator(SkipList* list) {
//   SetList(list);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline void SkipList<Key, Value>::Iterator::SetList(SkipList* list) {
//   list_ = list;
//   SeekToFirst();
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline bool SkipList<Key, Value>::Iterator::Valid() const {
//   return node_ != nullptr;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline const Key* SkipList<Key, Value>::Iterator::key() const {
//   assert(Valid());
//   return node_->key();
// }
// template <ComparableAndCopyble Key, Copyable Value>
// inline Value* SkipList<Key, Value>::Iterator::value() {
//   assert(Valid());
//   return node_->value();
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline bool SkipList<Key, Value>::Iterator::Next() {
//   assert(Valid());
//   node_ = node_->Next(0);
//   if (node_) {
//     return true;
//   }
//   return false;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline bool SkipList<Key, Value>::Iterator::Prev() {
//   // Instead of using explicit "prev" links, we just search for the
//   // last node that falls before key.
//   assert(Valid());
//   node_ = list_->FindLessThan(*node_->key());
//   if (node_ == list_->head_) {
//     node_ = nullptr;
//   }
//   if (node_) {
//     return true;
//   }
//   return false;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline bool SkipList<Key, Value>::Iterator::Delete() {
//   assert(Valid());
//   auto tmp = node_->Next(0);
//   auto r = list_->Delete(node_, list_->seq_splice_, true);
//   node_ = tmp;
//   if (node_ && r) {
//     return true;
//   }
//   return false;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline void SkipList<Key, Value>::Iterator::Seek(const Key& target) {
//   node_ = list_->FindGreaterOrEqual(target);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline void SkipList<Key, Value>::Iterator::SeekForPrev(const Key& target) {
//   Seek(target);
//   if (!Valid()) {
//     SeekToLast();
//   }
//   while (Valid() && target < *key()) {
//     Prev();
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline void SkipList<Key, Value>::Iterator::SeekToFirst() {
//   node_ = list_->head_->Next(0);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline void SkipList<Key, Value>::Iterator::SeekToLast() {
//   node_ = list_->FindLast();
//   if (node_ == list_->head_) {
//     node_ = nullptr;
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// inline auto SkipList<Key, Value>::GetIterator() {
//   Iterator it(this);
//   return it;
// }
// template <ComparableAndCopyble Key, Copyable Value>
// uint32_t SkipList<Key, Value>::Random() const {
//   static std::random_device rd;
//   static std::mt19937 gen(rd());
//   static std::uniform_int_distribution<uint32_t> dis(0, 10000);
//   return dis(gen);
// }
// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::NeedHightByRandom(float probability) const {
//   auto rd = Random();
//   return rd < probability * 10000;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// uint16_t SkipList<Key, Value>::RandomHeight() {
//   // Increase height with probability 1 in kBranching
//   int height = 1;
//   while (height < kMaxHeight_ && height < kMaxPossibleHeight &&
//          NeedHightByRandom(probability_)) {
//     height++;
//   }
//   assert(height > 0);
//   assert(height <= kMaxHeight_);
//   assert(height <= kMaxPossibleHeight);
//   return height;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::KeyIsAfterNode(const Key& key, Node* n) const {
//   // nullptr n is considered infinite
//   assert(n != head_);
//   return (n != nullptr) && (*n->key() < key);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// typename SkipList<Key, Value>::Node* SkipList<Key,
// Value>::FindGreaterOrEqual(
//     const Key& key) const {
//   // Note: It looks like we could reduce duplication by implementing
//   // this function as FindLessThan(key)->Next(0), but we wouldn't be able
//   // to exit early on equality and the result wouldn't even be correct.
//   // A concurrent insert might occur after FindLessThan(key) but before
//   // we get a chance to call Next(0).
//   Node* x = head_;
//   int level = GetMaxHeight() - 1;
//   Node* last_bigger = nullptr;
//   while (true) {
//     Node* next = x->Next(level);
//     if (next != nullptr) {
//       PREFETCH(next->Next(level), 0, 1);
//     }
//     // Make sure the lists are sorted
//     assert(x == head_ || next == nullptr || KeyIsAfterNode(*next->key(), x));
//     // Make sure we haven't overshot during our search
//     assert(x == head_ || KeyIsAfterNode(key, x));
//     int cmp = (next == nullptr || next == last_bigger)
//                   ? 1
//                   : (*next->key() == key ? 0 : (*next->key() < key ? -1 :
//                   1));
//     if (cmp == 0 || (cmp > 0 && level == 0)) {
//       return next;
//     } else if (cmp < 0) {
//       // Keep searching in this list
//       x = next;
//     } else {
//       // Switch to next list, reuse compare_() result
//       last_bigger = next;
//       level--;
//     }
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// typename SkipList<Key, Value>::Node* SkipList<Key, Value>::FindLessThan(
//     const Key& key, Node** prev) const {
//   return FindLessThan(key, prev, head_, GetMaxHeight(), 0);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// typename SkipList<Key, Value>::Node* SkipList<Key, Value>::FindLessThan(
//     const Key& key, Node** prev, Node* root, int top_level,
//     int bottom_level) const {
//   assert(top_level > bottom_level);
//   int level = top_level - 1;
//   Node* x = root;
//   // KeyIsAfter(key, last_not_after) is definitely false
//   Node* last_not_after = nullptr;
//   while (true) {
//     assert(x != nullptr);
//     Node* next = x->Next(level);
//     if (next != nullptr) {
//       PREFETCH(next->Next(level), 0, 1);
//     }
//     assert(x == head_ || next == nullptr || KeyIsAfterNode(*next->key(), x));
//     assert(x == head_ || KeyIsAfterNode(key, x));
//     if (next != last_not_after && KeyIsAfterNode(key, next)) {
//       // Keep searching in this list
//       assert(next != nullptr);
//       x = next;
//     } else {
//       if (prev != nullptr) {
//         prev[level] = x;
//       }
//       if (level == bottom_level) {
//         return x;
//       } else {
//         // Switch to next list, reuse KeyIsAfterNode() result
//         last_not_after = next;
//         level--;
//       }
//     }
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// typename SkipList<Key, Value>::Node* SkipList<Key, Value>::FindLast() const {
//   Node* x = head_;
//   int level = GetMaxHeight() - 1;
//   while (true) {
//     Node* next = x->Next(level);
//     if (next == nullptr) {
//       if (level == 0) {
//         return x;
//       } else {
//         // Switch to next list
//         level--;
//       }
//     } else {
//       x = next;
//     }
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// SkipList<Key, Value>::SkipList(uint32_t max_height, float probability)
//     : kMaxHeight_(static_cast<uint16_t>(max_height)),
//       probability_(probability),
//       head_(AllocateNode(max_height)),
//       max_height_(1),
//       seq_splice_(AllocateSplice()) {
//   assert(max_height > 0 && kMaxHeight_ == static_cast<uint32_t>(max_height));
//   assert(probability_ < 1 && probability_ > 0);
//   if (head_ == nullptr) {
//     throw std::runtime_error("skip list init failed because of OOM");
//   }

//   for (int i = 0; i < kMaxHeight_; ++i) {
//     head_->SetNext(i, nullptr);
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// SkipList<Key, Value>::~SkipList() {
//   DeallocateSplice(seq_splice_);
//   auto next = head_->Next(0);
//   DeallocateData(head_);
//   while (next != nullptr) {
//     auto tmp = next->Next(0);
//     DeallocateData(next);
//     next = tmp;
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// typename SkipList<Key, Value>::Node* SkipList<Key, Value>::AllocateData(
//     const Key& key, const Value& value) {
//   constexpr auto key_size = sizeof(Key);
//   constexpr auto value_size = sizeof(Value);
//   typename SkipList<Key, Value>::Node* node = AllocateNode(RandomHeight());
//   if (node == nullptr) {
//     return nullptr;
//   }
//   if constexpr (std::is_trivially_copyable<Key>::value) {
//     memcpy(node->KeyPos(), &key, key_size);
//   } else {
//     auto pkey = new (node->KeyPos()) Key(key);
//   }

//   if constexpr (std::is_trivially_copyable<Value>::value) {
//     memcpy(node->ValuePos(), &value, value_size);
//   } else {
//     auto pvalue = new (node->ValuePos()) Value(value);
//   }
//   return node;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// typename SkipList<Key, Value>::Node* SkipList<Key, Value>::AllocateNode(
//     int height) {
//   constexpr auto data_size = sizeof(Key) + sizeof(Value);
//   auto prefix = sizeof(Node*) * (height - 1);

//   // prefix is space for the height - 1 pointers that we store before
//   // the Node instance (next_[-(height - 1) .. -1]).  Node starts at
//   // raw + prefix, and holds the bottom-mode (level 0) skip list pointer
//   // next_[0].  key_size is the bytes for the key, which comes just after
//   // the Node.
//   char* raw = new (
//       std::nothrow) char[prefix + sizeof(Node) + data_size +
//       sizeof(uint32_t)];
//   if (raw == nullptr) {
//     return nullptr;
//   }
//   Node* x = reinterpret_cast<Node*>(raw + prefix);

//   x->SetHeight(height);
//   return x;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// typename SkipList<Key, Value>::Splice* SkipList<Key, Value>::AllocateSplice()
// {
//   // size of prev_ and next_
//   size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
//   char* raw = new (std::nothrow) char[sizeof(Splice) + array_size * 2];
//   if (raw == nullptr) {
//     return nullptr;
//   }
//   Splice* splice = reinterpret_cast<Splice*>(raw);
//   splice->height_ = 0;
//   splice->prev_ = reinterpret_cast<Node**>(raw + sizeof(Splice));
//   splice->next_ = reinterpret_cast<Node**>(raw + sizeof(Splice) +
//   array_size); return splice;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// void SkipList<Key, Value>::DeallocateData(Node* node) {
//   assert(node != nullptr);
//   if constexpr (std::is_destructible_v<Key>) {
//     Key* key_p = reinterpret_cast<Key*>(node->KeyPos());
//     key_p->~Key();
//   }
//   if constexpr (std::is_destructible_v<Value>) {
//     Value* value_p = reinterpret_cast<Value*>(node->ValuePos());
//     value_p->~Value();
//   }

//   auto height = node->height();
//   auto prefix = sizeof(Node*) * (height - 1);
//   char* raw = reinterpret_cast<char*>(node) - prefix;
//   delete[] raw;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// void SkipList<Key, Value>::DeallocateSplice(void* splice) {
//   assert(splice != nullptr);
//   char* buffer = reinterpret_cast<char*>(splice);
//   delete[] buffer;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::Insert(const Key& key, const Value& value) {
//   return Insert(key, value, seq_splice_, false);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::InsertBySequence(const Key& key, const Value&
// value,
//                                             Position& pos) {
//   return InsertWithHint(key, value, &pos.splice_);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::InsertWithHint(const Key& key, const Value& value,
//                                           Splice** hint) {
//   assert(hint != nullptr);
//   Splice* splice = *hint;
//   if (splice == nullptr) {
//     splice = AllocateSplice();
//     if (splice == nullptr) {
//       return false;
//     }
//     *hint = splice;
//   }
//   assert(splice != nullptr || splice->height_ >= 0);
//   return Insert(key, value, splice, true);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::Delete(const Key& key) {
//   auto node = FindGreaterOrEqual(key);
//   if (node == nullptr || !(*node->key() == key)) {
//     return true;
//   }
//   return Delete(node, seq_splice_, false);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::DeleteBySequence(const Key& key, Position& pos) {
//   return DeleteWithHint(key, &pos.splice_);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::DeleteWithHint(const Key key, Splice** hint) {
//   assert(hint != nullptr);
//   Splice* splice = *hint;
//   if (splice == nullptr) {
//     splice = AllocateSplice();
//     if (splice == nullptr) {
//       return false;
//     }
//     *hint = splice;
//   }
//   assert(splice != nullptr || splice->height_ >= 0);
//   auto node = FindGreaterOrEqual(key);
//   if (node == nullptr || !(*node->key() == key)) {
//     return true;
//   }
//   return Delete(node, seq_splice_, false);
// }

// template <ComparableAndCopyble Key, Copyable Value>
// void SkipList<Key, Value>::FindSpliceForLevel(const Key& key, Node* before,
//                                               Node* after, int level,
//                                               Node** out_prev,
//                                               Node** out_next) {
//   while (true) {
//     Node* next = before->Next(level);
//     if (next != nullptr) {
//       PREFETCH(next->Next(level), 0, 1);
//       if (level > 0) {
//         PREFETCH(next->Next(level - 1), 0, 1);
//       }
//     }
//     assert(before == head_ || next == nullptr ||
//            KeyIsAfterNode(*next->key(), before));
//     if (next == after || !KeyIsAfterNode(key, next)) {
//       // found it
//       *out_prev = before;
//       *out_next = next;
//       return;
//     }
//     before = next;
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// void SkipList<Key, Value>::RecomputeSpliceLevels(const Key& key, Splice*
// splice,
//                                                  int recompute_level) {
//   assert(recompute_level > 0);
//   assert(recompute_level <= splice->height_);
//   for (int i = recompute_level - 1; i >= 0; --i) {
//     FindSpliceForLevel(key, splice->prev_[i + 1], splice->next_[i + 1], i,
//                        &splice->prev_[i], &splice->next_[i]);
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// void SkipList<Key, Value>::FindSpliceForLevel(Node* target, Node* before,
//                                               Node* after, int level,
//                                               Node** out_prev,
//                                               Node** out_next) {
//   while (true) {
//     Node* next = before->Next(level);
//     if (next != nullptr) {
//       PREFETCH(next->Next(level), 0, 1);
//       if (level > 0) {
//         PREFETCH(next->Next(level - 1), 0, 1);
//       }
//     }
//     assert(before == head_ || next == nullptr ||
//            KeyIsAfterNode(*next->key(), before));
//     if (next == target || next == after ||
//         !KeyIsAfterNode(*target->key(), next)) {
//       // found it
//       *out_prev = before;
//       *out_next = next;
//       return;
//     }
//     before = next;
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// void SkipList<Key, Value>::RecomputeSpliceLevels(Node* target, Splice*
// splice,
//                                                  int recompute_level) {
//   assert(recompute_level > 0);
//   assert(recompute_level <= splice->height_);
//   for (int i = recompute_level - 1; i >= 0; --i) {
//     FindSpliceForLevel(target, splice->prev_[i + 1], splice->next_[i + 1], i,
//                        &splice->prev_[i], &splice->next_[i]);
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::Insert(const Key& key, const Value& value,
//                                   Splice* splice,
//                                   bool allow_partial_splice_fix) {
//   auto x = AllocateData(key, value);
//   if (x == nullptr) {
//     return false;
//   }
//   int height = x->height();
//   assert(height >= 1 && height <= kMaxHeight_);

//   if (height > max_height_) {
//     max_height_ = height;
//   }
//   assert(max_height_ <= kMaxPossibleHeight);

//   int recompute_height = 0;
//   if (splice->height_ < max_height_) {
//     // Either splice has never been used or max_height has grown since
//     // last use.  We could potentially fix it in the latter case, but
//     // that is tricky.
//     splice->prev_[max_height_] = head_;
//     splice->next_[max_height_] = nullptr;
//     splice->height_ = max_height_;
//     recompute_height = max_height_;
//   } else {
//     // Splice is a valid proper-height splice that brackets some
//     // key, but does it bracket this one?  We need to validate it and
//     // recompute a portion of the splice (levels 0..recompute_height-1)
//     // that is a superset of all levels that don't bracket the new key.
//     // Several choices are reasonable, because we have to balance the work
//     // saved against the extra comparisons required to validate the Splice.
//     //
//     // One strategy is just to recompute all of orig_splice_height if the
//     // bottom level isn't bracketing.  This pessimistically assumes that
//     // we will either get a perfect Splice hit (increasing sequential
//     // inserts) or have no locality.
//     //
//     // Another strategy is to walk up the Splice's levels until we find
//     // a level that brackets the key.  This strategy lets the Splice
//     // hint help for other cases: it turns insertion from O(log N) into
//     // O(log D), where D is the number of nodes in between the key that
//     // produced the Splice and the current insert (insertion is aided
//     // whether the new key is before or after the splice).  If you have
//     // a way of using a prefix of the key to map directly to the closest
//     // Splice out of O(sqrt(N)) Splices and we make it so that splices
//     // can also be used as hints during read, then we end up with Oshman's
//     // and Shavit's SkipTrie, which has O(log log N) lookup and insertion
//     // (compare to O(log N) for skip list).
//     //
//     // We control the pessimistic strategy with allow_partial_splice_fix.
//     // A good strategy is probably to be pessimistic for seq_splice_,
//     // optimistic if the caller actually went to the work of providing
//     // a Splice.
//     while (recompute_height < max_height_) {
//       if (splice->prev_[recompute_height]->Next(recompute_height) !=
//           splice->next_[recompute_height]) {
//         // splice isn't tight at this level, there must have been some
//         inserts
//         // to this
//         // location that didn't update the splice.  We might only be a little
//         // stale, but if
//         // the splice is very stale it would be O(N) to fix it.  We haven't
//         used
//         // up any of
//         // our budget of comparisons, so always move up even if we are
//         // pessimistic about
//         // our chances of success.
//         ++recompute_height;
//       } else if (splice->prev_[recompute_height] != head_ &&
//                  !KeyIsAfterNode(key, splice->prev_[recompute_height])) {
//         // key is from before splice
//         if (allow_partial_splice_fix) {
//           // skip all levels with the same node without more comparisons
//           Node* bad = splice->prev_[recompute_height];
//           while (splice->prev_[recompute_height] == bad) {
//             ++recompute_height;
//           }
//         } else {
//           // we're pessimistic, recompute everything
//           recompute_height = max_height_;
//         }
//       } else if (KeyIsAfterNode(key, splice->next_[recompute_height])) {
//         // key is from after splice
//         if (allow_partial_splice_fix) {
//           Node* bad = splice->next_[recompute_height];
//           while (splice->next_[recompute_height] == bad) {
//             ++recompute_height;
//           }
//         } else {
//           recompute_height = max_height_;
//         }
//       } else {
//         // this level brackets the key, we won!
//         break;
//       }
//     }
//   }
//   assert(recompute_height <= max_height_);
//   if (recompute_height > 0) {
//     RecomputeSpliceLevels(key, splice, recompute_height);
//   }

//   bool splice_is_valid = true;
//   for (int i = 0; i < height; ++i) {
//     if (i >= recompute_height &&
//         splice->prev_[i]->Next(i) != splice->next_[i]) {
//       FindSpliceForLevel(key, splice->prev_[i], nullptr, i,
//       &splice->prev_[i],
//                          &splice->next_[i]);
//     }
//     // Checking for duplicate keys on the level 0 is sufficient
//     if (i == 0 && splice->next_[i] != nullptr &&
//         (*(splice->next_[i]->key()) < *(x->key()) ||
//          *(x->key()) == *(splice->next_[i]->key()))) [[unlikely]] {
//       // duplicate key
//       DeallocateData(x);
//       return false;
//     }
//     if (i == 0 && splice->prev_[i] != head_ &&
//         (*(x->key()) < *(splice->prev_[i]->key()) ||
//          *(splice->prev_[i]->key()) == *(x->key()))) [[unlikely]] {
//       // duplicate key
//       DeallocateData(x);
//       return false;
//     }
//     assert(splice->next_[i] == nullptr ||
//            *x->key() < *(splice->next_[i]->key()));
//     assert(splice->prev_[i] == head_ || *(splice->prev_[i]->key()) <
//     *x->key()); assert(splice->prev_[i]->Next(i) == splice->next_[i]);
//     x->SetNext(i, splice->next_[i]);
//     splice->prev_[i]->SetNext(i, x);
//   }
//   if (splice_is_valid) {
//     for (int i = 0; i < height; ++i) {
//       splice->prev_[i] = x;
//     }
//     assert(splice->prev_[splice->height_] == head_);
//     assert(splice->next_[splice->height_] == nullptr);
//     for (int i = 0; i < splice->height_; ++i) {
//       assert(splice->next_[i] == nullptr || key <
//       *(splice->next_[i]->key())); assert(splice->prev_[i] == head_ ||
//       *(splice->prev_[i]->key()) < key ||
//              *(splice->prev_[i]->key()) == key);
//       assert(splice->prev_[i + 1] == splice->prev_[i] ||
//              splice->prev_[i + 1] == head_ ||
//              *(splice->prev_[i + 1]->key()) < *(splice->prev_[i]->key()));
//       assert(splice->next_[i + 1] == splice->next_[i] ||
//              splice->next_[i + 1] == nullptr ||
//              *(splice->next_[i]->key()) < *(splice->next_[i + 1]->key()));
//     }
//   } else {
//     splice->height_ = 0;
//   }
//   return true;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::Delete(Node* target_node, Splice* splice,
//                                   bool allow_partial_splice_fix) {
//   int recompute_height = 0;
//   if (splice->height_ < max_height_) {
//     splice->prev_[max_height_] = head_;
//     splice->next_[max_height_] = nullptr;
//     splice->height_ = max_height_;
//     recompute_height = max_height_;
//   } else {
//     while (recompute_height < max_height_) {
//       if (splice->prev_[recompute_height]->Next(recompute_height) !=
//           splice->next_[recompute_height]) {
//         ++recompute_height;
//       } else if (splice->prev_[recompute_height] != head_ &&
//                  !KeyIsAfterNode(*target_node->key(),
//                                  splice->prev_[recompute_height])) {
//         // key is from before splice
//         if (allow_partial_splice_fix) {
//           // skip all levels with the same node without more comparisons
//           Node* bad = splice->prev_[recompute_height];
//           while (splice->prev_[recompute_height] == bad) {
//             ++recompute_height;
//           }
//         } else {
//           // we're pessimistic, recompute everything
//           recompute_height = max_height_;
//         }
//       } else if (KeyIsAfterNode(*target_node->key(),
//                                 splice->next_[recompute_height])) {
//         // key is from after splice
//         if (allow_partial_splice_fix) {
//           Node* bad = splice->next_[recompute_height];
//           while (splice->next_[recompute_height] == bad) {
//             ++recompute_height;
//           }
//         } else {
//           recompute_height = max_height_;
//         }
//       } else {
//         // this level brackets the key, we won!
//         break;
//       }
//     }
//   }
//   assert(recompute_height <= max_height_);
//   if (recompute_height > 0) {
//     RecomputeSpliceLevels(target_node, splice, recompute_height);
//   }
//   for (int i = 0; i < max_height_; ++i) {
//     if (i >= recompute_height &&
//         splice->prev_[i]->Next(i) != splice->next_[i]) {
//       FindSpliceForLevel(target_node, splice->prev_[i], nullptr, i,
//                          &splice->prev_[i], &splice->next_[i]);
//     }
//     if (splice->next_[i] != nullptr && splice->next_[i] == target_node) {
//       splice->prev_[i]->SetNext(i, splice->next_[i]->Next(i));
//       splice->next_[i] = splice->next_[i]->Next(i);
//     }
//   }
//   DeallocateData(target_node);
//   return true;
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::Contains(const Key& key) const {
//   Node* x = FindGreaterOrEqual(key);
//   if (x != nullptr && key == *x->key()) {
//     return true;
//   } else {
//     return false;
//   }
// }

// template <ComparableAndCopyble Key, Copyable Value>
// bool SkipList<Key, Value>::Find(const Key& key, Value** value) {
//   Node* x = FindGreaterOrEqual(key);
//   if (x != nullptr && key == *x->key()) {
//     *value = x->value();
//     return true;
//   } else {
//     return false;
//   }
// }

// class Test {
//  public:
//   friend bool operator==(const Test& l, const Test& r) { return false; };

//   friend bool operator<(const Test& l, const Test& r) { return false; }
//   explicit Test(int i) : i(i) {}
//   ~Test() = default;
//   // Test(const Test&) {}
//   // Test& operator=(const Test&) {}

//   // Test(Test&&) = delete;
//   // Test& operator=(Test&&) = delete;
//   int get() { return i; }

//  private:
//   int i{0};
// };

// class Solution {
//  public:
//   std::vector<int> searchRange(std::vector<int>& nums, int target) {

//     int l = 0;
//     int r = nums.size() - 1;
//     while (l < r) {
//       int m = (l + r) / 2;
//       if (nums[m] > target) {
//         r = m;
//       } else if (nums[m] < target) {
//         l = m + 1;
//       } else {
//         int front_r = m;
//         if (nums[l] != target) {
//           while (l < front_r) {
//             int front_m = (l + front_r) / 2;
//             if (nums[front_m] < target) {
//               l = front_m + 1;
//               if (l < nums.size() && nums[l] == target) {
//                 break;
//               }
//             } else {
//               front_r = front_m;
//             }
//           }
//         }

//         while (m < r) {
//           int after_m = (r + m) / 2;
//           if (nums[after_m] == target) {
//             m = after_m + 1;
//             if (m < nums.size() && nums[m] != target) {
//               r = m - 1;
//               break;
//             }
//           } else {
//             r = after_m;
//           }
//         }
//         break;
//       }
//     }
//     if (l < r) {
//       return {l, r};
//     } else if (l == r && nums[l] == target) {
//       return {l, l};
//     } else {
//       return {-1, -1};
//     }
//   }
// };

// int main() {
// }
