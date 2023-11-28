#include "echo_server_tf.h"

#include <fmt/format.h>
#include <grpcpp/support/status.h>

#include "absl/status/status.h"
#include "echo.pb.h"
#include "options.h"
#include "seastar/core/future.hh"
#include "seastar/core/loop.hh"
#include "seastar/core/sharded.hh"
#include "seastar/core/smp.hh"
#include "seastar/util/log.hh"
#include "seastar_app.h"

// This macro creates a new request for the given RPC method name
// (e.g., `ENQUEUE_REQUEST(GetStatus, false);`), and enqueues it on
// `this->cq_`.
//
// This macro is invoked one or more times for each RPC method to
// ensure that there are sufficient completion queue entries to
// handle incoming requests without blocking.
//
// The implementation of the request handler for each RPC method
// must ensure that it calls ENQUEUE_REQUEST() for that RPC method,
// to keep accepting new requests.

// Call<GrpcWorkerServiceThread, grpc::WorkerService::AsyncService,
//    method##Request, method##Response>::
#define ENQUEUE_REQUEST(method, supports_cancel)                            \
  do {                                                                      \
    if (!is_shutdown_) {                                                    \
      Call<GrpcEchoService, echo::EchoService::AsyncService,                \
           echo::method##Request, echo::method##Response>::                 \
          EnqueueRequest(&service_, cq_.get(),                              \
                         &echo::EchoService::AsyncService::Request##method, \
                         &GrpcEchoService::method##Handler,                 \
                         (supports_cancel));                                \
    }                                                                       \
  } while (0)

seastar::logger lg("echo server");

seastar::future<> GrpcEchoService::HandleRPCsLoop() {
  // TODO(ncteisen): This may require performance engineering. We can
  // change the number of threads, the number of handlers per thread,
  // or even decide to specialize certain threads to certain methods.
  ENQUEUE_REQUEST(Echo, false);

  void* tag;
  bool ok;
  bool is_stop = false;
  return seastar::do_until(
      [&is_stop] { return is_stop; },
      [&]() {
        is_stop = !cq_->Next(&tag, &ok);
        if (tag) {
          (static_cast<UntypedCall<GrpcEchoService>::Tag*>(tag))
              ->OnCompleted(this, ok);
        } else {
          // NOTE(mrry): A null `callback_tag` indicates that this
          // is the shutdown alarm.
          cq_->Shutdown();
        }
        return seastar::make_ready_future();
      });
}

seastar::future<> GrpcEchoService::Shutdown() {
  return seastar::make_ready_future()
      .then([this] {
        if (!is_shutdown_) {
          return impl_->stop();
        } else {
          return seastar::make_ready_future();
        }
      })
      .then([this] {
        if (!is_shutdown_) {
          cq_->Shutdown();
          is_shutdown_ = true;
        }
        return seastar::make_ready_future();
      });
}

seastar::future<> GrpcEchoService::EchoHandler(
    EchoCall<echo::EchoRequest, echo::EchoResponse>* call) {
  return impl_->local()
      .EchoHandler(&call->request, &call->response)
      .then([call](Status&& s) {
        ::grpc::Status gs;
        if (s.ok()) {
          gs = ::grpc::Status::OK;
        } else {
          gs = ::grpc::Status::CANCELLED;
        }
        return call->SendResponse(gs);
      })
      .then([this] {
        ENQUEUE_REQUEST(Echo, false);
        return seastar::make_ready_future();
      });
}

seastar::future<> EchoServiceImpl::Start() {
  lg.info("echo imple start, shard {}/{}", seastar::this_shard_id(),
          seastar::smp::count);
  return seastar::make_ready_future();
}
Status EchoServiceImpl::Init() {
  lg.info("echo imple init, shard {}/{}", seastar::this_shard_id(),
          seastar::smp::count);
  return absl::OkStatus();
}
seastar::future<> EchoServiceImpl::stop() {
  lg.info("echo imple stop, shard {}/{}", seastar::this_shard_id(),
          seastar::smp::count);
  return seastar::make_ready_future();
}
seastar::future<Status> EchoServiceImpl::Echo(const echo::EchoRequest* request,
                                              echo::EchoResponse* resources) {
  lg.info("echo imple echo, shard {}/{}", seastar::this_shard_id(),
          seastar::smp::count);
  auto m = request->message();
  auto r = fmt::format("hello {}", m);
  resources->set_message(r);
  return seastar::make_ready_future().then([]() { return absl::OkStatus(); });
}
seastar::future<Status> EchoServiceImpl::EchoHandler(
    const echo::EchoRequest* request, echo::EchoResponse* resources) {
  lg.info("echo imple echo handler, shard {}/{}", seastar::this_shard_id(),
          seastar::smp::count);
  static int i = 0;
  auto home_shard = i++ % seastar::smp::count;
  if (home_shard == seastar::this_shard_id()) {
    // Fast path: we are the home shard for this address, can
    // probably get a token locally (unless exhausted)
    return Echo(request, resources);
  } else {
    return container().invoke_on(home_shard,
                                 [request, resources](EchoServiceImpl& impl) {
                                   return impl.Echo(request, resources);
                                 });
  }
}

Status EchoService::Init(const ::grpc::ServerBuilder* builder) {
  Options options{};

  auto impl = std::make_unique<seastar::sharded<EchoServiceImpl>>();
  impl->Init();
  return impl->start()
      .than([impl = std::move(impl)]() {
        return impl->invoke_on_all(&EchoServiceImpl::Start);
      })
      .than([this] {
        grpcService_ =
            std::make_unique<GrpcEchoService>(builder, impl, options);
        return absl::OkStatus();
      });
}

seastar::future<> EchoService::Start() {
  co_return grpcService_->HandleRPCsLoop();
}

seastar::future<> EchoService::Stop() { co_return grpcService_->Shutdown(); }

int main(int ac, char** av) {
  AppOptions options{ac, av};
  auto app = SeastarApp::GetInstance();
  auto s = app->Init();
  if (!s.ok()) {
  }
  return app->Run(options);
}
