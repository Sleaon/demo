#include "rpc.h"


template <typename Verb_t>
class RpcServerImple : public RpcServer<Verb_t>{
    config _cfg;
    // map: Node broadcast address -> Node internal IP, and the reversed mapping, for communication within the same data center
    std::unordered_map<gms::inet_address, gms::inet_address> _preferred_ip_cache, _preferred_to_endpoint;
    std::unique_ptr<rpc_protocol_wrapper> _rpc;
    std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;
    std::unique_ptr<seastar::tls::credentials_builder> _credentials_builder;
    std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server_tls;
    std::vector<clients_map> _clients;
    uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::LAST)] = {};
    bool _shutting_down = false;
    connection_drop_signal_t _connection_dropped;
    scheduling_config _scheduling_config;
    std::vector<scheduling_info_for_connection_index> _scheduling_info_for_connection_index;
    std::vector<tenant_connection_index> _connection_index_for_tenant;

    struct connection_ref;
    std::unordered_multimap<locator::host_id, connection_ref> _host_connections;
    std::unordered_set<locator::host_id> _banned_hosts;

    future<> shutdown_tls_server();
    future<> shutdown_nontls_server();
    future<> stop_tls_server();
    future<> stop_nontls_server();
    future<> stop_client();
 public:
  seastar::future<> start(){

  }
  seastar::future<> shutdown(){

  }
  seastar::future<> stop() = 0;
  uint16_t port(){

  }
  seastar::net::inet_address listen_address() = 0;
  std::unique_ptr<rpc_protocol_wrapper<Verb_t>>& rpc() = 0;
  std::shared_ptr<rpc_protocol_client_wrapper<Verb_t>> get_rpc_client(
      Verb_t verb, Addr id) = 0;
  bool is_shutting_down() = 0;
  void RemoveErrorRpcClient(Verb_t verb, Addr id);
  void RemoveRpcClient(Addr id);

} ;