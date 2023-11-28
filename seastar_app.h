#ifndef TENSORFLOW_CONTRIB_STAR_SEASTAR_SEASTAR_ENGINE_H_
#define TENSORFLOW_CONTRIB_STAR_SEASTAR_SEASTAR_ENGINE_H_

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "seastar/core/app-template.hh"
#include "seastar/core/smp.hh"

typedef absl::Status Status;
class ServiceInterface;

class ServerFactory {
 public:
  // Creates a new server based on the given `server_def`, and stores
  // it in `*out_server`. Returns OK on success, otherwise returns an
  // error.
  virtual Status NewSharedServer(std::string_view service_name,
                                 ServiceInterface** out_server) = 0;
  virtual ~ServerFactory() {}

  // For each `ServerFactory` subclass, an instance of that class must
  // be registered by calling this method.
  //
  // The `server_type` must be unique to the server factory.
  static void Register(std::string_view service_name, ServerFactory* factory);

  // Looks up a factory that can create a server based on the given
  // `server_def`, and stores it in `*out_factory`. Returns OK on
  // success, otherwise returns an error.
  static Status GetFactory(std::string_view service_name,
                           ServerFactory** out_factory);
};
struct AppOptions;

class SeastarApp {
 public:
  static SeastarApp* GetInstance() {
    static SeastarApp app;
    return &app;
  }
  Status Init();
  int Run(const AppOptions& options);

  virtual ~SeastarApp();

  // seastar::co GetChannel();
 private:
  SeastarApp()
      : init_ready_(false),
        core_number_(seastar::smp::count),
        servers_({}),
        builder_(nullptr),
        app_(){};
  SeastarApp(const SeastarApp&) = delete;
  SeastarApp& operator=(const SeastarApp&) = delete;
  Status ServersInit();
  void Stop();

 private:
  ::std::atomic<bool> init_ready_;
  size_t core_number_;
  ::std::vector<ServiceInterface*> servers_;
  ::grpc::ServerBuilder* builder_;
  seastar::app_template app_;
  std::unique_ptr<::grpc::Server> grpc_server_;
};

#endif  // TENSORFLOW_CONTRIB_STAR_SEASTAR_SEASTAR_ENGINE_H_
