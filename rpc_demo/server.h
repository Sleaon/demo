#ifndef SERVER_H_
#define SERVER_H_
#include "seastar/core/future.hh"


class ServiceInterface {
 public:
  virtual ~ServiceInterface() {}
  virtual void start() = 0;
  // stop() will be invoked by the Seastar framework, and thus it does not
  // adhere to Google Style.
  virtual seastar::future<> stop() = 0;
};

#endif  // TENSORFLOW_CORE_DISTRIBUTED_RUNTIME_RPC_GRPC_WORKER_SERVICE_H_