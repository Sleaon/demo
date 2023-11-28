#ifndef SERVER_H_
#define SERVER_H_

#include <grpcpp/server_builder.h>

#include "absl/status/status.h"
#include "seastar/core/future.hh"

typedef absl::Status Status;

class ServiceInterface {
 public:
  virtual ~ServiceInterface() {}
  virtual Status Init(const ::grpc::ServerBuilder* builder) = 0;
  virtual seastar::future<> Start() = 0;
  // stop() will be invoked by the Seastar framework, and thus it does not
  // adhere to Google Style.
  virtual seastar::future<> Stop() = 0;
};

class GrpcServiceInterface {
 public:
  GrpcServiceInterface() = default;
  virtual ~GrpcServiceInterface() {}

  // A blocking method that should be called to handle incoming RPCs.
  // This method will block until the service shuts down.
  virtual seastar::future<> HandleRPCsLoop() = 0;

  // Starts shutting down this service.
  //
  // NOTE(mrry): To shut down this service completely, the caller must
  // also shut down any servers that might share ownership of this
  // service's resources (e.g. completion queues).
  virtual seastar::future<> Shutdown() = 0;
};

#endif  // TENSORFLOW_CORE_DISTRIBUTED_RUNTIME_RPC_GRPC_WORKER_SERVICE_H_