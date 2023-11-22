#ifndef SERVER_H_
#define SERVER_H_

#include <memory>
#include <unordered_map>

class ServiceInterface {
 public:
  virtual ~ServiceInterface() {}

  virtual void Start() = 0;

  virtual void Stop() = 0;
};

class GrpcServiceInterface{
public:
  virtual ~GrpcServiceInterface() {}

  // A blocking method that should be called to handle incoming RPCs.
  // This method will block until the service shuts down.
  virtual void HandleRPCsLoop() = 0;

  // Starts shutting down this service.
  //
  // NOTE(mrry): To shut down this service completely, the caller must
  // also shut down any servers that might share ownership of this
  // service's resources (e.g. completion queues).
  virtual void Shutdown() = 0;
};


#endif  // TENSORFLOW_CORE_DISTRIBUTED_RUNTIME_RPC_GRPC_WORKER_SERVICE_H_