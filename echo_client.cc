#include <grpc/support/log.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "echo.grpc.pb.h"
#endif

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
std::atomic_uint64_t id = 0;

auto NowTime() {
  auto sysnow = std::chrono::system_clock::now();
  auto now = sysnow.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

auto kstart_time = NowTime();
std::atomic_uint64_t kend_time = NowTime();

class EchoClient {
 public:
  explicit EchoClient(std::shared_ptr<Channel> channel)
      : stub_(echo::EchoService::NewStub(channel)), cq_pool_(10) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  void Echo(const std::string& msg) {
    // Data we are sending to the server.
    echo::EchoRequest request;
    request.set_message(msg);
    AsyncClientCall* call = new AsyncClientCall;
    auto& cq = Schedule(call->id_);

    call->response_reader =
        stub_->PrepareAsyncEcho(&call->context, request, &cq);
    call->response_reader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the integer 1.
    call->response_reader->Finish(&call->reply, &call->status, call);
  }

  void AsyncCompleteRpc(int id) {
    void* got_tag;
    bool ok = false;
    auto& cq = Schedule(id);

    // 在队列为空时阻塞，队列中有响应结果时读取到got_tag和ok两个参数
    // 前者是结果对应的RPC请求的地址，后者是响应的状态
    while (cq.Next(&got_tag, &ok)) {
      // 类型转换，获取到的实际上是此响应结果对应的RPC请求的地址，在这个地址下保存了实际的响应结果数据
      AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

      // 验证请求是否真的完成了
      GPR_ASSERT(ok);

      if (call->status.ok()) {
        auto end_time = NowTime();
        kend_time.store(end_time);
        std::cout << "Echo received: " << call->reply.message()
                  << " cost: " << end_time - call->start_time_ << "ms"
                  << std::endl;
        if (call->id_ == 999) {
          std::cout << "all cost: " << kend_time.load() - kstart_time
                    << std::endl;
        }
      } else
        std::cout << "RPC failed" << std::endl;
      delete call;
    }
  }

 private:
  CompletionQueue& Schedule(int id) {
    auto index = id % cq_pool_.size();
    return cq_pool_[index];
  }
  struct AsyncClientCall {
    uint64_t id_ = id.fetch_add(1);
    uint64_t start_time_ = NowTime();
    // 服务器返回的响应数据
    echo::EchoResponse reply;

    // 客户端的上下文信息，可以被用于向服务器传达额外信息或调整某些RPC行为
    ClientContext context;

    // RPC响应的状态
    Status status;

    // 客户端异步响应读取器
    std::unique_ptr<ClientAsyncResponseReader<echo::EchoResponse>>
        response_reader;
  };
  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<echo::EchoService::Stub> stub_;
  std::vector<CompletionQueue> cq_pool_;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  std::string target_str = "localhost:50051";
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).
  EchoClient echo(
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
  std::vector<std::thread> pool;
  for (int i = 0; i < 10; i++) {
    pool.emplace_back([&echo, i]() { echo.AsyncCompleteRpc(i); });
  }
  kstart_time = NowTime();
  for (int i = 0; i < 1000; i++) {
    std::string user("world " + std::to_string(i));
    echo.Echo(user);
  }
  auto end_time = NowTime();
  std::cout << "echo request complete, cost: " << end_time - kstart_time
            << std::endl;
  for (auto&& t : pool) {
    t.join();
  }
  return 0;
}