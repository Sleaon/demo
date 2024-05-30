#include "seastar_app.h"

#include <fmt/format.h>

#include <boost/program_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/reactor.hh>
#include <vector>

#include "server.h"
#include "options.h"

namespace bpo = boost::program_options;

typedef std::lock_guard<std::mutex> l_guard;
std::mutex* get_server_factory_lock() {
  static std::mutex server_factory_lock;
  return &server_factory_lock;
}

typedef std::unordered_map<std::string, ServerFactory*> ServerFactories;
ServerFactories* server_factories() {
  static ServerFactories* factories = new ServerFactories;
  return factories;
}

void ServerFactory::Register(std::string_view service_name,
                             ServerFactory* factory) {
  l_guard l(*get_server_factory_lock());
  ServerFactories().emplace(service_name, factory);
}

int ServerFactory::GetFactory(std::string_view service_name,
                                 ServerFactory** out_factory) {
  l_guard l(*get_server_factory_lock());
  auto it = server_factories()->find(service_name.data());
  if (it == server_factories()->end()) {
    return 1;
  }
  *out_factory = it->second;
  return 0;
}

int SeastarApp::Init() {
  app_.add_options()(
      "port", bpo::value<uint16_t>()->default_value(9999),
      "Specify UDP and TCP ports for memcached server to listen on");

  auto s = ServersInit();
  if (!s) {
    return s;
  }
  init_ready_.store(true);
  return 0;
}

int SeastarApp::ServersInit() {
  std::vector<std::string> service_list = {"echo_service"};
  for (auto&& service : service_list) {
    ServerFactory* factory;
    auto s = ServerFactory::GetFactory(service, &factory);
    if (!s) {
      return s;
    }
    ServiceInterface* server;
    s = factory->NewSharedServer(service, &server);
    if (!s) {
      return s;
    }
    servers_.emplace_back(server);
  }
  return 0;
}

int SeastarApp::Run(const AppOptions& options) {
  if (init_ready_.load() != true) {
    return 1;
  }
  return app_.run_deprecated(options.argc, options.argv, [&] {
    for (auto&& server : servers_) {
      seastar::engine().at_exit([&server] { return server->Stop(); });
    }

    auto&& config = app_.configuration();
    uint16_t port = config["port"].as<uint16_t>();
    return seastar::parallel_for_each(
        servers_, [](ServiceInterface* server) { return server->Start(); });
  });
}

SeastarApp::~SeastarApp() { }