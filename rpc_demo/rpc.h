#pragma once

#ifndef RPC_H_
#define RPC_H_
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>
#include <seastar/rpc/rpc.hh>

#include "seastar/core/abort_source.hh"
#include "serializer.h"

template <typename Verb_t>
class rpc_protocol_wrapper;

template <typename Verb_t>
class rpc_protocol_client_wrapper;
struct Addr;

template <typename Verb_t>
class RpcServer : public seastar::async_sharded_service<RpcServer<Verb_t>>,
                  public seastar::peering_sharded_service<RpcServer<Verb_t>> {
 public:
  seastar::future<> start() = 0;
  seastar::future<> shutdown() = 0;
  seastar::future<> stop() = 0;
  uint16_t port() = 0;
  seastar::net::inet_address listen_address() = 0;
  std::unique_ptr<rpc_protocol_wrapper<Verb_t>>& rpc() = 0;
  std::shared_ptr<rpc_protocol_client_wrapper<Verb_t>> get_rpc_client(
      Verb_t verb, Addr id) = 0;
  bool is_shutting_down() = 0;
  void RemoveErrorRpcClient(Verb_t verb, Addr id);
  void RemoveRpcClient(Addr id);
};

struct SerializerPlaceholder {};

// thunk from rpc serializers to generate serializers
template <typename T, typename Output>
void write(SerializerPlaceholder, Output& out, const T& data) {
  serialize(out, data);
}
template <typename T, typename Input>
T read(SerializerPlaceholder, Input& in, boost::type<T> type) {
  return deserialize(in, type);
}

template <typename Output, typename T>
void write(SerializerPlaceholder s, Output& out,
           const seastar::foreign_ptr<T>& v) {
  return write(s, out, *v);
}
template <typename Input, typename T>
seastar::foreign_ptr<T> read(SerializerPlaceholder s, Input& in,
                             boost::type<seastar::foreign_ptr<T>>) {
  return make_foreign(read(s, in, boost::type<T>()));
}

template <typename Output, typename T>
void write(SerializerPlaceholder s, Output& out,
           const seastar::lw_shared_ptr<T>& v) {
  return write(s, out, *v);
}
template <typename Input, typename T>
seastar::lw_shared_ptr<T> read(SerializerPlaceholder s, Input& in,
                               boost::type<seastar::lw_shared_ptr<T>>) {
  return make_lw_shared<T>(read(s, in, boost::type<T>()));
}

template <typename Verb_t>
using rpc_protocol = seastar::rpc::protocol<SerializerPlaceholder, Verb_t>;

template <typename Verb_t>
class rpc_protocol_wrapper {
  rpc_protocol<Verb_t> _impl;

 public:
  explicit rpc_protocol_wrapper(SerializerPlaceholder&& s)
      : _impl(std::move(s)) {}

  auto& protocol() { return _impl; }

  template <typename Func>
  auto make_client(Verb_t t) {
    return _impl.template make_client<Func>(t);
  }

  template <typename Func>
  auto register_handler(Verb_t t, Func&& func) {
    return _impl.register_handler(t, std::forward<Func>(func));
  }

  template <typename Func>
  auto register_handler(Verb_t t, seastar::scheduling_group sg, Func&& func) {
    return _impl.register_handler(t, sg, std::forward<Func>(func));
  }

  seastar::future<> unregister_handler(Verb_t t) {
    return _impl.unregister_handler(t);
  }

  void set_logger(::seastar::logger* logger) { _impl.set_logger(logger); }

  bool has_handler(Verb_t msg_id) { return _impl.has_handler(msg_id); }

  bool has_handlers() const noexcept { return _impl.has_handlers(); }
};

// This wrapper pretends to be rpc_protocol::client, but also handles
// stopping it before destruction, in case it wasn't stopped already.
// This should be integrated into messaging_service proper.
template <typename Verb_t>
class rpc_protocol_client_wrapper {
  std::unique_ptr<typename rpc_protocol<Verb_t>::client> _p;
  seastar::shared_ptr<seastar::tls::server_credentials> _credentials;

 public:
  rpc_protocol_client_wrapper(rpc_protocol<Verb_t>& proto,
                              seastar::rpc::client_options opts,
                              seastar::socket_address addr,
                              seastar::socket_address local = {})
      : _p(std::make_unique<rpc_protocol<Verb_t>::client>(
            proto, std::move(opts), addr, local)) {}

  rpc_protocol_client_wrapper(
      rpc_protocol<Verb_t>& proto, seastar::rpc::client_options opts,
      seastar::socket_address addr, seastar::socket_address local,
      seastar::shared_ptr<seastar::tls::server_credentials> c)
      : _p(std::make_unique<rpc_protocol<Verb_t>::client>(
            proto, std::move(opts), seastar::tls::socket(c), addr, local)),
        _credentials(c) {}

  auto get_stats() const { return _p->get_stats(); }

  seastar::future<> stop() { return _p->stop(); }

  bool error() { return _p->error(); }

  operator typename rpc_protocol<Verb_t>::client &() { return *_p; }

  /**
   * #3787 Must ensure we use the right type of socket. I.e. tls or not.
   * See above, we retain credentials object so we here can know if we
   * are tls or not.
   */
  template <typename Serializer, typename... Out>
  seastar::future<seastar::rpc::sink<Out...>> make_stream_sink() {
    if (_credentials) {
      return _p->template make_stream_sink<Serializer, Out...>(
          seastar::tls::socket(_credentials));
    }
    return _p->template make_stream_sink<Serializer, Out...>();
  }
};

struct Addr {
  seastar::net::inet_address addr;
  uint32_t cpu_id;
  friend bool operator==(const Addr& x, const Addr& y) noexcept;
  friend bool operator<(const Addr& x, const Addr& y) noexcept;
  friend std::ostream& operator<<(std::ostream& os, const Addr& x);
  struct hash {
    size_t operator()(const Addr& id) const noexcept;
  };
  explicit Addr(seastar::net::inet_address ip) noexcept : addr(ip), cpu_id(0) {}
  Addr(seastar::net::inet_address ip, uint32_t cpu) noexcept
      : addr(ip), cpu_id(cpu) {}
};

bool operator==(const Addr& x, const Addr& y) noexcept {
  // Ignore cpu id for now since we do not really support shard to shard
  // connections
  return x.addr == y.addr;
}

bool operator<(const Addr& x, const Addr& y) noexcept {
  // Ignore cpu id for now since we do not really support shard to shard
  // connections
  if (x.addr.data() < y.addr.data()) {
    return true;
  } else {
    return false;
  }
}

std::ostream& operator<<(std::ostream& os, const Addr& x) {
  fmt::print(os, "{}:{}", x.addr, x.cpu_id);
  return os;
}

// Register a handler (a callback lambda) for verb
// template<typename Verb_t, typename Func>
// void register_handler(RpcServer<Verb_t> *rpcserver, verb_t verb,
// Func &&func) {
//     rpcserver->rpc()->register_handler(verb,
//     rpcserver->scheduling_group_for_verb(verb), std::move(func));
// }

template <typename Verb_t, typename Func>
void register_handler(RpcServer<Verb_t>* rpcserver, Verb_t verb, Func&& func) {
  rpcserver->rpc()->register_handler(verb, std::move(func));
}

// Send a message for verb
template <typename Verb_t, typename MsgIn, typename... MsgOut>
auto send_message(RpcServer<Verb_t>* rpcserver, Verb_t verb, Addr id,
                  MsgOut&&... msg) {
  auto rpc_handler =
      rpcserver->rpc()->template make_client<MsgIn(MsgOut...)>(verb);
  using futurator = seastar::futurize<std::result_of_t<decltype(rpc_handler)(
      typename rpc_protocol<Verb_t>::client&, MsgOut...)>>;
  if (rpcserver->is_shutting_down()) {
    return futurator::make_exception_future(seastar::rpc::closed_error());
  }
  auto rpc_client_ptr = rpcserver->get_rpc_client(verb, id);
  auto& rpc_client = *rpc_client_ptr;
  return rpc_handler(rpc_client, std::forward<MsgOut>(msg)...)
      .handle_exception([rs = rpcserver->shared_from_this(), id, verb,
                         rpc_client_ptr = std::move(rpc_client_ptr)](
                            std::exception_ptr&& eptr) {
        try {
          std::rethrow_exception(eptr);
        } catch (seastar::rpc::closed_error& t) {
          rs->RemoveErrorRpcClient(verb, id);
          return futurator::make_exception_future(std::move(eptr));

        } catch (...) {
          return futurator::make_exception_future(std::move(eptr));
        }
      });
}

// TODO: Remove duplicated code in send_message
template <typename Verb_t, typename MsgIn, typename Timeout, typename... MsgOut>
auto send_message_timeout(RpcServer<Verb_t>* rpcserver, Verb_t verb, Addr id,
                          Timeout timeout, MsgOut&&... msg) {
  auto rpc_handler =
      rpcserver->rpc()->template make_client<MsgIn(MsgOut...)>(verb);
  using futurator = seastar::futurize<std::result_of_t<decltype(rpc_handler)(
      typename rpc_protocol<Verb_t>::client&, MsgOut...)>>;
  if (rpcserver->is_shutting_down()) {
    return futurator::make_exception_future(seastar::rpc::closed_error());
  }
  auto rpc_client_ptr = rpcserver->get_rpc_client(verb, id);
  auto& rpc_client = *rpc_client_ptr;
  return rpc_handler(rpc_client, timeout, std::forward<MsgOut>(msg)...)
      .handle_exception([rs = rpcserver->shared_from_this(), id, verb,
                         rpc_client_ptr = std::move(rpc_client_ptr)](
                            std::exception_ptr&& eptr) {
        try {
          std::rethrow_exception(eptr);
        } catch (seastar::rpc::closed_error& t) {
          rs->RemoveErrorRpcClient(verb, id);
          return futurator::make_exception_future(std::move(eptr));

        } catch (...) {
          return futurator::make_exception_future(std::move(eptr));
        }
      });
}

// Requesting abort on the provided abort_source drops the message from the
// outgoing queue (if it's still there) and causes the returned future to
// resolve exceptionally with `abort_requested_exception`.
// TODO: Remove duplicated code in send_message
template <typename Verb_t, typename MsgIn, typename... MsgOut>
auto send_message_cancellable(RpcServer<Verb_t>* rpcserver, Verb_t verb,
                              Addr id, seastar::abort_source& as,
                              MsgOut&&... msg) {
  auto rpc_handler =
      rpcserver->rpc()->template make_client<MsgIn(MsgOut...)>(verb);
  using futurator = seastar::futurize<std::result_of_t<decltype(rpc_handler)(
      typename rpc_protocol<Verb_t>::client&, MsgOut...)>>;
  if (rpcserver->is_shutting_down()) {
    return futurator::make_exception_future(seastar::rpc::closed_error());
  }
  auto rpc_client_ptr = rpcserver->get_rpc_client(verb, id);
  auto& rpc_client = *rpc_client_ptr;

  auto c = std::make_unique<seastar::rpc::cancellable>();
  auto& c_ref = *c;
  auto sub = as.subscribe([c = std::move(c)]() noexcept { c->cancel(); });
  if (!sub) {
    return futurator::make_exception_future(
        seastar::abort_requested_exception{});
  }

  return rpc_handler(rpc_client, c_ref, std::forward<MsgOut>(msg)...)
      .handle_exception([rs = rpcserver->shared_from_this(), id, verb,
                         rpc_client_ptr = std::move(rpc_client_ptr),
                         sub = std::move(sub)](std::exception_ptr&& eptr) {
        try {
          std::rethrow_exception(eptr);
        } catch (seastar::rpc::closed_error& close_e) {
          rs->RemoveErrorRpcClient(verb, id);
          return futurator::make_exception_future(std::move(eptr));

        } catch (seastar::rpc::canceled_error& cancel_e) {
          return futurator::make_exception_future(
              seastar::abort_requested_exception{});
        } catch (...) {
          return futurator::make_exception_future(std::move(eptr));
        }
      });
}

// Send one way message for verb
template <typename Verb_t, typename... MsgOut>
auto send_message_oneway(RpcServer<Verb_t>* rpcserver, Verb_t verb, Addr id,
                         MsgOut&&... msg) {
  return send_message<seastar::rpc::no_wait_type>(
      rpcserver, std::move(verb), std::move(id), std::forward<MsgOut>(msg)...);
}

// Send one way message for verb
template <typename Verb_t, typename Timeout, typename... MsgOut>
auto send_message_oneway_timeout(RpcServer<Verb_t>* rpcserver, Verb_t verb,
                                 Addr id, Timeout timeout, MsgOut&&... msg) {
  return send_message_timeout<seastar::rpc::no_wait_type>(
      rpcserver, std::move(verb), std::move(id), timeout,
      std::forward<MsgOut>(msg)...);
}

struct config {
  seastar::net::inet_address ip;
  uint16_t port;
  uint16_t ssl_port = 0;
  bool listen_on_broadcast_address = false;
  size_t rpc_memory_limit = 1'000'000;
};
#endif