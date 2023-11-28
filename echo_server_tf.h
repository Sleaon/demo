#ifndef ECHO_SERVER_TF_H_
#define ECHO_SERVER_TF_H_

#include <memory>

#include "echo.grpc.pb.h"
#include "echo.pb.h"
#include "grpcpp/alarm.h"
#include "options.h"
#include "seastar/core/future.hh"
#include "seastar/core/sharded.hh"
#include "server.h"
#include "call.h"

class GrpcEchoService;
class EchoServiceImpl;
class EchoService;

typedef std::unique_ptr<seastar::sharded<EchoServiceImpl>> ShardedServicePtr;

class EchoService : public ServiceInterface {
 public:
  EchoService(){};
  virtual ~EchoService() {}

  Status Init(const ::grpc::ServerBuilder* builder) override;

  seastar::future<> Start() override;

  seastar::future<> Stop() override;

 private:
  std::unique_ptr<GrpcEchoService> grpcService_;
};

class EchoServiceImpl : public seastar::peering_sharded_service<EchoServiceImpl> {
 public:
  EchoServiceImpl(){};
  virtual ~EchoServiceImpl() {}

  virtual seastar::future<> Start();
  virtual Status Init();
  virtual seastar::future<> stop();
  seastar::future<Status> EchoHandler(const echo::EchoRequest* request,echo::EchoResponse* resources);

 private:
  seastar::future<Status> Echo(const echo::EchoRequest* request,echo::EchoResponse* resources);
};

class GrpcEchoService : public GrpcServiceInterface {
 public:
  GrpcEchoService(::grpc::ServerBuilder* builder, ShardedServicePtr impl,
                  const Options& options)
      : impl_(std::move(impl)),
        builder_(builder),
        options_(options),
        shutdown_alarm_(nullptr),
        is_shutdown_(false),
        cq_(std::move(builder->AddCompletionQueue())),
        service_() {
    builder->RegisterService(&service_);
  };
  virtual ~GrpcEchoService() { delete shutdown_alarm_; }

  // A blocking method that should be called to handle incoming RPCs.
  // This method will block until the service shuts down.
  virtual seastar::future<> HandleRPCsLoop() override;

  // Starts shutting down this service.
  //
  // NOTE(mrry): To shut down this service completely, the caller must
  // also shut down any servers that might share ownership of this
  // service's resources (e.g. completion queues).
  virtual seastar::future<> Shutdown() override;

 private:
  seastar::future<> EchoHandler(
      Call<GrpcEchoService, echo::EchoService::AsyncService,echo::EchoRequest, echo::EchoResponse>* call);

 private:
  ShardedServicePtr impl_;
  const ::grpc::ServerBuilder* builder_;
  Options options_;
  ::grpc::Alarm* shutdown_alarm_;
  bool is_shutdown_;
  std::unique_ptr<::grpc::ServerCompletionQueue> cq_;
  echo::EchoService::AsyncService service_;
};
#endif  // TENSORFLOW_CORE_DISTRIBUTED_RUNTIME_RPC_GRPC_WORKER_SERVICE_H_