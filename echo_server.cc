#include "echo_server.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#include "echo.grpc.pb.h"

std::atomic<uint64_t> EchoContext::id = 0;

ServerImpl::~ServerImpl() {
  server_->Shutdown();
  // Always shutdown the completion queue after the server.
  for (auto&& cq : cq_pool_) {
    cq->Shutdown();
  }
}

// There is no shutdown handling in this code.
void ServerImpl::Run(uint16_t port) {
  std::string server_address = "0.0.0.0:" + std::to_string(port);

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service_" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *asynchronous* service.
  builder.RegisterService(&service_);
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  for (int i = 0; i < 10; ++i) {
    cq_pool_.emplace_back(builder.AddCompletionQueue());
  }
  // Finally assemble the server.
  server_ = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;

  auto context = new EchoContext();
  Proceed(context);
  // Proceed to the server's main loop.
  for (int i = 0; i < 10; ++i) {
    pool.emplace_back([this, i]() { HandleRpcs(i); });
  }
  for (auto&& thread : pool) {
    thread.join();
  }
}

grpc::Status ServerImpl::Echo(grpc::ServerContext* context,
                              const echo::EchoRequest* request,
                              echo::EchoResponse* response) {
  std::this_thread::sleep_for(
      std::chrono::duration(std::chrono::milliseconds(200)));
  std::ostringstream os;
  os << "hello " << request->message() << " " << GetTid();
  std::string message = os.str();
  std::cout << message << std::endl;
  response->set_message(message);
  // grpc状态可以设置message,所以也可以用来返回一些信息
  return grpc::Status(grpc::StatusCode::OK, message);
}

void ServerImpl::Proceed(EchoContext* context) {
  std::cout << "id: " << context->id_ << " status: " << context->status_
            << std::endl;
  if (context->status_ == CREATE) {
    std::cout << "id: " << context->id_ << " Create " << std::endl;
    // Make this instance progress to the PROCESS state.
    context->status_ = PROCESS;
    auto& cq = Schedule(context->id_);

    // As part of the initial CREATE state, we *request* that the system
    // start processing SayHello requests. In this request, "this" acts are
    // the tag uniquely identifying the request (so that different CallData
    // instances can serve different requests concurrently), in this case
    // the memory address of this CallData instance.
    service_.RequestEcho(&context->ctx_, &context->request_,
                         &context->responder_, cq.get(), cq.get(), context);
  } else if (context->status_ == PROCESS) {
    std::cout << "id: " << context->id_ << " process " << std::endl;
    // Spawn a new CallData instance to serve new clients while we process
    // the one for this CallData. The instance will deallocate itself as
    // part of its FINISH state.
    // The actual processing.

    auto status = Echo(&context->ctx_, &context->request_, &context->response_);
    // And we are done! Let the gRPC runtime know we've finished, using the
    // memory address of this instance as the uniquely identifying tag for
    // the event.
    auto new_context = new EchoContext();
    Proceed(new_context);
    context->status_ = FINISH;
    context->responder_.Finish(context->response_, status, context);
  } else {
    std::cout << "id: " << context->id_ << " other" << std::endl;
    GPR_ASSERT(context->status_ == FINISH);
    // Once in the FINISH state, deallocate ourselves (CallData).
    delete context;
  }
}
// This can be run in multiple threads if needed.
void ServerImpl::HandleRpcs(int index) {
  // Spawn a new CallData instance to serve new clients.
  EchoContext* tag = nullptr;
  bool ok;
  int i = 0;
  auto& cq = Schedule(index);

  while (true) {
    std::cout << "handle rpc " << i++ << std::endl;

    // Block waiting to read the next event from the completion queue. The
    // event is uniquely identified by its tag, which in this case is the
    // memory address of a CallData instance.
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or cq_ is shutting down.
    GPR_ASSERT(cq->Next((void**)&tag, &ok));
    GPR_ASSERT(ok);
    std::cout << "get cq" << std::endl;
    Proceed(tag);
  }
}

int main(int argc, char** argv) {
  ServerImpl server;
  server.Run(50051);

  return 0;
}