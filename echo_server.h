#include <atomic>
#include <cstdint>
#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>

#include "echo.pb.h"

#include "echo.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

enum CallStatus { CREATE, PROCESS, FINISH };

struct EchoContext {
  static std::atomic<uint64_t> id;
  uint64_t id_;
  // 当前处理状态（处理分为两步：1处理请求构建响应数据；2发送响应）
  // 这里记录一下完成到哪一步了，以便进行相关操作
  CallStatus status_;  // (1构建响应完成；2发送完成)
  // rpc的上下文，允许通过它进行诸如压缩、身份验证，以及把元数据发回客户端等。
  grpc::ServerContext ctx_;
  echo::EchoRequest request_;
  echo::EchoResponse response_;
  grpc::ServerAsyncResponseWriter<echo::EchoResponse> responder_;
  //================================================
  // 构造函数
  EchoContext()
      : id_(id.load(std::memory_order_release)),status_(CREATE), ctx_(), request_(), response_(), responder_(&ctx_) {id.fetch_add(1, std::memory_order_acquire);}
};

unsigned long GetTid() {
  std::thread::id tid = std::this_thread::get_id();
  std::ostringstream os;
  os << tid;
  unsigned long tidx = std::stol(os.str());
  return tidx;
}

class ServerImpl final {
 public:
  ~ServerImpl();
  // There is no shutdown handling in this code.
  void Run(uint16_t port);
  grpc::Status Echo(grpc::ServerContext* context,
                    const echo::EchoRequest* request,
                    echo::EchoResponse* response);

 private:
  void Proceed(EchoContext* context);
  void HandleRpcs(int id);
  auto& Schedule(int id) {
    auto index = id % cq_pool_.size();
    return cq_pool_[index];
  }
  echo::EchoService::AsyncService service_;
  std::unique_ptr<Server> server_;
  std::vector<std::thread> pool;
  std::vector<std::unique_ptr<ServerCompletionQueue>> cq_pool_;
};