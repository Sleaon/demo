/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <array>
#include <seastar/core/distributed.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/tls.hh>
#include <seastar/rpc/rpc_types.hh>
#include <string>
#include <unordered_map>
#include <vector>

#include "msg_addr.h"
#include "rpc_verb.h"

namespace rpcserver {
using namespace seastar;

typedef std::string hostname;

struct serializer {};

class messaging_service
    : public seastar::async_sharded_service<messaging_service>,
      public peering_sharded_service<messaging_service> {
 public:
  class rpc_protocol_wrapper;
  class rpc_protocol_client_wrapper;
  class rpc_protocol_server_wrapper;
  struct shard_info;

  using inet_address = seastar::net::inet_address;
  using clients_map = std::unordered_map<msg_addr, shard_info, msg_addr::hash>;

  // This should change only if serialization format changes
  static constexpr int32_t current_version = 0;

  struct shard_info {
    shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client);
    shared_ptr<rpc_protocol_client_wrapper> rpc_client;
    rpc::stats get_stats() const;
  };

  void foreach_client(
      std::function<void(const msg_addr& id, const shard_info& info)> f) const;

  int32_t get_raw_version(const inet_address& endpoint) const;

  bool knows_version(const inet_address& endpoint) const;

  enum class tcp_nodelay_what {
    local,
    all,
  };

  struct config {
    hostname id;
    seastar::net::inet_address ip;
    uint16_t port;
    tcp_nodelay_what tcp_nodelay = tcp_nodelay_what::all;
    bool listen_on_broadcast_address = false;
    size_t rpc_memory_limit = 1'000'000;
  };

  struct connection_ref {
    // Refers to one of the servers in `messaging_service::_server` or
    // `messaging_service::_server_tls`.
    rpc::server& server;

    // Refers to a connection inside `server`.
    rpc::connection_id conn_id;
  };

 private:
  config _cfg;
  // map: Node broadcast address -> Node internal IP, and the reversed mapping,
  // for communication within the same data center
  std::unique_ptr<rpc_protocol_wrapper> _rpc;
  std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server;
  std::vector<clients_map> _clients;
  bool _shutting_down = false;
  std::unordered_multimap<hostname, connection_ref> _host_connections;
  std::unordered_set<hostname> _banned_hosts;

  future<> shutdown_nontls_server();
  future<> stop_nontls_server();
  future<> stop_client();

 public:
  using clock_type = lowres_clock;

  messaging_service(hostname id, inet_address ip, uint16_t port);
  messaging_service(config cfg);
  ~messaging_service();

  future<> start();
  future<> start_listen();
  uint16_t port();
  inet_address listen_address();
  future<> shutdown();
  future<> stop();
  static rpc::no_wait_type no_wait();
  bool is_shutting_down() { return _shutting_down; }

  future<> unregister_handler(RpcVerb verb);

  void foreach_server_connection_stats(
      std::function<void(const rpc::client_info&, const rpc::stats&)>&& f)
      const;

  // Drops all connections from the given host and prevents further
  // communication from it to happen.
  //
  // No further RPC handlers will be called for that node,
  // but we don't prevent handlers that were started concurrently from
  // finishing.
  future<> ban_host(std::string);

 private:
  template <typename Fn>
  requires std::is_invocable_r_v<bool, Fn, const shard_info&>
  void find_and_remove_client(clients_map& clients, msg_addr id, Fn&& filter);
  void do_start_listen();

  bool is_host_banned(hostname);


 public:
  // Return rpc::protocol::client for a shard which is a ip + cpuid pair.
  shared_ptr<rpc_protocol_client_wrapper> get_rpc_client(RpcVerb verb,
                                                         msg_addr id);
  void remove_error_rpc_client(RpcVerb verb, msg_addr id);
  void remove_rpc_client(msg_addr id);
  std::unique_ptr<rpc_protocol_wrapper>& rpc();
  static msg_addr get_source(const rpc::client_info& client);
  unsigned get_rpc_client_idx(RpcVerb verb) const;
};

}  // namespace rpcserver
