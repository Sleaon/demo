#ifndef ECHO_SERVER_TF_H_
#define ECHO_SERVER_TF_H_

#include <memory>
#include <unordered_map>

#include "call.h"
#include "echo.pb.h"
#include "server.h"
#include "echo.grpc.pb.h"

struct EchoCall: public Call{
    EchoCall():Call("Echo"){}
    echo::EchoRequest request;
    static std::atomic<uint64_t> id;
    uint64_t id_;
    grpc::ServerContext ctx_;
    echo::EchoRequest request_;
    echo::EchoResponse response_;
    grpc::ServerAsyncResponseWriter<echo::EchoResponse> responder_;
};

class EchoService : public ServiceInterface {
 public:
  EchoService(){};
  virtual ~EchoService() {}

  void Start() override;

  void Stop() override;
};

class GrpcEchoService : public GrpcServiceInterface {
 public:
  virtual ~GrpcEchoService() {}

  // A blocking method that should be called to handle incoming RPCs.
  // This method will block until the service shuts down.
  void HandleRPCsLoop() override;

  // Starts shutting down this service.
  //
  // NOTE(mrry): To shut down this service completely, the caller must
  // also shut down any servers that might share ownership of this
  // service's resources (e.g. completion queues).
  void Shutdown() override;
};

#endif  // TENSORFLOW_CORE_DISTRIBUTED_RUNTIME_RPC_GRPC_WORKER_SERVICE_H_