/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "messaging_service.h"

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/rpc/lz4_compressor.hh>
#include <seastar/rpc/lz4_fragmented_compressor.hh>
#include <seastar/rpc/multi_algo_compressor_factory.hh>
#include <seastar/rpc/rpc.hh>
#include <sstream>

#include "rpc_protocol_impl.h"
#include "rpc_verb.h"

namespace rpcserver {

static_assert(!std::is_default_constructible_v<msg_addr>);
static_assert(std::is_nothrow_copy_constructible_v<msg_addr>);
static_assert(std::is_nothrow_move_constructible_v<msg_addr>);

static seastar::logger mlogger("messaging_service");
static seastar::logger rpc_logger("rpc");

using inet_address = seastar::net::inet_address;
using namespace std::chrono_literals;

static rpc::lz4_fragmented_compressor::factory
    lz4_fragmented_compressor_factory;
static rpc::lz4_compressor::factory lz4_compressor_factory;
static rpc::multi_algo_compressor_factory compressor_factory{
    &lz4_fragmented_compressor_factory,
    &lz4_compressor_factory,
};

struct messaging_service::rpc_protocol_server_wrapper
    : public rpc_protocol::server {
  using rpc_protocol::server::server;
};

constexpr int32_t messaging_service::current_version;

// Count of connection types that are not associated with any tenant
const size_t PER_SHARD_CONNECTION_COUNT = 2;
// Counts per tenant connection types
const size_t PER_TENANT_CONNECTION_COUNT = 3;

bool operator==(const msg_addr& x, const msg_addr& y) noexcept {
  // Ignore cpu id for now since we do not really support shard to shard
  // connections
  return x.addr == y.addr;
}

bool operator<(const msg_addr& x, const msg_addr& y) noexcept {
  // Ignore cpu id for now since we do not really support shard to shard
  // connections
  if (x.addr.data() < y.addr.data()) {
    return true;
  } else {
    return false;
  }
}

std::ostream& operator<<(std::ostream& os, const msg_addr& x) {
  fmt::print(os, "{}:{}", x.addr, x.cpu_id);
  return os;
}

size_t msg_addr::hash::operator()(const msg_addr& id) const noexcept {
  // Ignore cpu id for now since we do not really support // shard to shard
  // connections
  return std::hash<seastar::net::inet_address>()(id.addr);
}

messaging_service::shard_info::shard_info(
    shared_ptr<rpc_protocol_client_wrapper>&& client)
    : rpc_client(std::move(client)) {}

rpc::stats messaging_service::shard_info::get_stats() const {
  return rpc_client->get_stats();
}

void messaging_service::foreach_client(
    std::function<void(const msg_addr& id, const shard_info& info)> f) const {
  for (unsigned idx = 0; idx < _clients.size(); idx++) {
    for (auto i = _clients[idx].cbegin(); i != _clients[idx].cend(); i++) {
      f(i->first, i->second);
    }
  }
}

void messaging_service::foreach_server_connection_stats(
    std::function<void(const rpc::client_info&, const rpc::stats&)>&& f) const {
  for (auto&& s : _server) {
    if (s) {
      s->foreach_connection([f](const rpc_protocol::server::connection& c) {
        f(c.info(), c.get_stats());
      });
    }
  }
}

int32_t messaging_service::get_raw_version(const inet_address& endpoint) const {
  // FIXME: messaging service versioning
  return current_version;
}

bool messaging_service::knows_version(const inet_address& endpoint) const {
  // FIXME: messaging service versioning
  return true;
}

future<> messaging_service::unregister_handler(RpcVerb verb) {
  return _rpc->unregister_handler(verb);
}

messaging_service::messaging_service(hostname id, inet_address ip,
                                     uint16_t port)
    : messaging_service(config{std::move(id), std::move(ip), port}) {}

static rpc::resource_limits rpc_resource_limits(size_t memory_limit) {
  rpc::resource_limits limits;
  limits.bloat_factor = 3;
  limits.basic_request_size = 1000;
  limits.max_memory = memory_limit;
  return limits;
}

future<> messaging_service::start() { return make_ready_future<>(); }

future<> messaging_service::start_listen() {
  do_start_listen();
  return make_ready_future<>();
}

future<> messaging_service::ban_host(hostname id) {
  return container().invoke_on_all([id](messaging_service& ms) {
    if (ms._banned_hosts.contains(id) || ms.is_shutting_down()) {
      return;
    }

    ms._banned_hosts.insert(id);
    auto [start, end] = ms._host_connections.equal_range(id);
    for (auto it = start; it != end; ++it) {
      auto& conn_ref = it->second;
      conn_ref.server.abort_connection(conn_ref.conn_id);
    }
    ms._host_connections.erase(start, end);
  });
}

bool messaging_service::is_host_banned(hostname id) {
  return _banned_hosts.contains(id);
}

void messaging_service::do_start_listen() {
  bool listen_to_bc =
      _cfg.listen_on_broadcast_address && _cfg.ip != inet_address();
  rpc::server_options so;
  so.compressor_factory = &compressor_factory;
  so.load_balancing_algorithm =
      server_socket::load_balancing_algorithm::connection_distribution;

  // FIXME: we don't set so.tcp_nodelay, because we can't tell at this point
  // whether the connection will come from a
  //        local or remote datacenter, and whether or not the connection will
  //        be used for gossip. We can fix the first by wrapping its
  //        server_socket, but not the second.
  auto limits = rpc_resource_limits(_cfg.rpc_memory_limit);
  if (!_server[0] && _cfg.port) {
    auto listen = [&](const inet_address& a,
                      rpc::streaming_domain_type sdomain) {
      so.streaming_domain = sdomain;
      so.filter_connection = {};
      auto addr = socket_address{a, _cfg.port};
      return std::unique_ptr<rpc_protocol_server_wrapper>(
          new rpc_protocol_server_wrapper(_rpc->protocol(), so, addr, limits));
    };
    _server[0] = listen(_cfg.ip, rpc::streaming_domain_type(0x55AA));
    if (listen_to_bc) {
      _server[1] = listen(inet_address(), rpc::streaming_domain_type(0x66BB));
    }
  }

  // Do this on just cpu 0, to avoid duplicate logs.
  if (this_shard_id() == 0) {
    if (_server[0]) {
      mlogger.info("Starting Messaging Service on port {}", _cfg.port);
    }
    if (_server[1]) {
      mlogger.info("Starting Messaging Service on broadcast address {} port {}",
                   inet_address(), _cfg.port);
    }
  }
}

messaging_service::messaging_service(config cfg)
    : _cfg(std::move(cfg)),
      _rpc(new rpc_protocol_wrapper(serializer{})),
      _clients(PER_SHARD_CONNECTION_COUNT + PER_TENANT_CONNECTION_COUNT) {
  _rpc->set_logger(&rpc_logger);

  register_handler(
      this, RpcVerb::kMessgingService_ClientID,
      [](rpc::client_info& ci, inet_address broadcast_address,
         uint32_t src_cpu_id, rpc::optional<uint64_t> max_result_size,
         hostname host_id) {
        ci.attach_auxiliary("baddr", broadcast_address);
        ci.attach_auxiliary("src_cpu_id", src_cpu_id);
        ci.attach_auxiliary("max_result_size",
                            max_result_size.value_or(1 * 1024 * 1024));
        ci.attach_auxiliary("host_id", host_id);
        return rpc::no_wait;
      });
}

msg_addr messaging_service::get_source(const rpc::client_info& cinfo) {
  return msg_addr{cinfo.retrieve_auxiliary<inet_address>("baddr"),
                  cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id")};
}

messaging_service::~messaging_service() = default;

uint16_t messaging_service::port() { return _cfg.port; }

inet_address messaging_service::listen_address() { return _cfg.ip; }

static future<> do_with_servers(
    std::string_view what,
    std::array<std::unique_ptr<messaging_service::rpc_protocol_server_wrapper>,
               2>& servers,
    auto method) {
  mlogger.info("{} server", what);
  co_await coroutine::parallel_for_each(
      servers | boost::adaptors::filtered([](auto& ptr) { return bool(ptr); }) |
          boost::adaptors::indirected,
      method);
  mlogger.info("{} server - Done", what);
}

future<> messaging_service::shutdown_nontls_server() {
  return do_with_servers("Shutting down nontls", _server,
                         std::mem_fn(&rpc_protocol_server_wrapper::shutdown));
}

future<> messaging_service::stop_nontls_server() {
  return do_with_servers("Stopping nontls", _server,
                         std::mem_fn(&rpc_protocol_server_wrapper::stop));
}

future<> messaging_service::stop_client() {
  return parallel_for_each(_clients, [](auto& m) {
    return parallel_for_each(
               m,
               [](std::pair<const msg_addr, shard_info>& c) {
                 mlogger.info("Stopping client for address: {}", c.first.addr);
                 return c.second.rpc_client->stop().then([addr = c.first] {
                   mlogger.info("Stopping client for address: {} - Done", addr.addr);
                 });
               })
        .finally([&m] {
          // no new clients should be added by get_rpc_client(), as it
          // asserts that _shutting_down is true
          m.clear();
          mlogger.info("Stopped clients");
        });
  });
}

future<> messaging_service::shutdown() {
  _shutting_down = true;
  co_await when_all(shutdown_nontls_server(), stop_client()).discard_result();
}

future<> messaging_service::stop() {
  if (!_shutting_down) {
    co_await shutdown();
  }
  co_await stop_nontls_server();
  co_await unregister_handler(RpcVerb::kMessgingService_ClientID);
  if (_rpc->has_handlers()) {
    mlogger.error("RPC server still has handlers registered");
    for (auto verb = RpcVerb(0); verb < RpcVerb(RpcVerb::kLast);
         verb = RpcVerb(uint64_t(verb) + 1)) {
      if (_rpc->has_handler(verb)) {
        mlogger.error(" - {}", static_cast<int>(verb));
      }
    }

    std::abort();
  }
}

rpc::no_wait_type messaging_service::no_wait() { return rpc::no_wait; }

unsigned messaging_service::get_rpc_client_idx(RpcVerb verb) const {
  return uint64_t(verb) % PER_TENANT_CONNECTION_COUNT +
         PER_SHARD_CONNECTION_COUNT;
}

shared_ptr<messaging_service::rpc_protocol_client_wrapper>
messaging_service::get_rpc_client(RpcVerb verb, msg_addr id) {
  assert(!_shutting_down);
  auto idx = get_rpc_client_idx(verb);
  auto it = _clients[idx].find(id);

  if (it != _clients[idx].end()) {
    auto c = it->second.rpc_client;
    if (!c->error()) {
      return c;
    }
    // The 'dead_only' it should be true, because we're interested in
    // dropping the errored socket, but since it's errored anyway (the
    // above if) it's false to save unneeded second c->error() call
    find_and_remove_client(_clients[idx], id, [](const auto&) { return true; });
  }

  auto my_host_id = _cfg.id;
  auto broadcast_address = inet_address();
  bool listen_to_bc =
      _cfg.listen_on_broadcast_address && _cfg.ip != broadcast_address;
  auto laddr = socket_address(listen_to_bc ? broadcast_address : _cfg.ip, 0);

  auto must_tcp_nodelay = [&] {
    // See comment above `TOPOLOGY_INDEPENDENT_IDX`.
    if (_cfg.tcp_nodelay == tcp_nodelay_what::all) {
      return true;
    }
    return false;
  }();

  auto addr = id.addr;
  auto remote_addr = socket_address(addr, _cfg.port);

  rpc::client_options opts;
  // send keepalive messages each minute if connection is idle, drop connection
  // after 10 failures
  opts.keepalive = std::optional<net::tcp_keepalive_params>({60s, 60s, 10});
  opts.compressor_factory = &compressor_factory;
  opts.tcp_nodelay = must_tcp_nodelay;
  opts.reuseaddr = true;

  auto client = make_shared<rpc_protocol_client_wrapper>(
      _rpc->protocol(), std::move(opts), remote_addr, laddr);

  // Remember if we had the peer's topology information when creating the
  // client; if not, we shall later drop the client and create a new one after
  // we learn the peer's topology (so we can use optimal encryption settings and
  // so on for intra-dc/rack messages). But we don't want to apply this logic
  // for TOPOLOGY_INDEPENDENT_IDX client - its settings are independent of
  // topology, so there's no point in dropping it later after we learn the
  // topology (so we always set `topology_ignored` to `false` in that case).
  auto res = _clients[idx].emplace(id, shard_info(std::move(client)));
  assert(res.second);
  it = res.first;
  uint32_t src_cpu_id = this_shard_id();
  // No reply is received, nothing to wait for.
  (void)_rpc
      ->make_client<rpc::no_wait_type(inet_address, uint32_t, uint64_t,
                                      hostname)>(
          RpcVerb::kMessgingService_ClientID)(*it->second.rpc_client,
                                              inet_address(), src_cpu_id,
                                              1 * 1024 * 1024, my_host_id)
      .handle_exception([ms = shared_from_this(), remote_addr,
                         verb](std::exception_ptr ep) {
        mlogger.debug("Failed to send client id to {} for verb {}: {}",
                      remote_addr, std::underlying_type_t<RpcVerb>(verb), ep);
      });
  return it->second.rpc_client;
}

template <typename Fn>
requires std::is_invocable_r_v<bool, Fn, const messaging_service::shard_info&>
void messaging_service::find_and_remove_client(clients_map& clients,
                                               msg_addr id, Fn&& filter) {
  if (_shutting_down) {
    // if messaging service is in a processed of been stopped no need to
    // stop and remove connection here since they are being stopped already
    // and we'll just interfere
    return;
  }

  auto it = clients.find(id);
  if (it != clients.end() && filter(it->second)) {
    auto client = std::move(it->second.rpc_client);
    clients.erase(it);
    //
    // Explicitly call rpc_protocol_client_wrapper::stop() for the erased
    // item and hold the messaging_service shared pointer till it's over.
    // This will make sure messaging_service::stop() blocks until
    // client->stop() is over.
    //
    (void)client->stop()
        .finally([id, client, ms = shared_from_this()] {
          mlogger.debug("dropped connection to {}", id.addr);
        })
        .discard_result();
  }
}

void messaging_service::remove_error_rpc_client(RpcVerb verb, msg_addr id) {
  find_and_remove_client(_clients[get_rpc_client_idx(verb)], id,
                         [](const auto& s) { return s.rpc_client->error(); });
}

void messaging_service::remove_rpc_client(msg_addr id) {
  for (auto& c : _clients) {
    find_and_remove_client(c, id, [](const auto&) { return true; });
  }
}

std::unique_ptr<messaging_service::rpc_protocol_wrapper>&
messaging_service::rpc() {
  return _rpc;
}

}  // namespace rpcserver
