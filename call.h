/* Copyright 2016 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef ECHO_CALL_H_
#define ECHO_CALL_H_

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>

#include "seastar/core/future.hh"
#include "seastar/core/shared_mutex.hh"

// CALL STRUCTURES
// ===============
//
// Each pending (incoming) request corresponds to a call object that
// encapsulates the state of the call. Templates and
// pointers-to-member functions are used to avoid boilerplate and
// redundant closure creation. The class hierarchy is as follows:
//
// * `UntypedCall<Service>`: The base class represents a call that
//   could be associated with any of the methods on a service of type
//   `Service`. Also defines a `Tag` nested class that can be used as
//   the tag in a `grpc::CompletionQueue`.  Each class that
//   instantiates `Service` should have a completion queue polling
//   loop that knows about `UntypedCall<Service>::Tag` objects, and
//   invokes their `OnCompleted()` method to continue processing.
//
// * `Call<Service, GrpcService, Req, Resp>`: This class extends
//   `UntypedCall<Service>` and is additionally parameterized by the
//   gRPC-generated asynchronous service class, and the request and
//   response message types. It defines the state associated with a
//   call (whose type depends on the message types), and stores a
//   pointer to a `Service::HandleFoo()` handler method. Each
//   `Service::HandleFoo()` method knows about the corresponding
//   `Call` type, in order to access its state, and invoke its
//   `SendResponse()` method.
//
// The lifecycle of a call object is as follows.
//
// 1. A `Service` creates a `Call` for a particular method and
//    enqueues it in its completion queue (via an
//    `UntypedCall<Service>::Tag`).
//
// 2. When the tag is returned from `cq_->Next()`, the
//    `UntypedCall::RequestReceived()` method is invoked and takes
//    ownership of the call object. This indirectly invokes the
//    appropriate handler method on `Service`.
//
// 3. After the response has been written (perhaps in another thread),
//    the `Call::SendResponse()` method is invoked. It transfers
//    ownership of the call object back to the completion queue (via
//    an `UntypedCall::Tag`).
//
// 4. When the response has been sent, the tag is returned from
//    `cq_->Next()`, and the call object is deleted.
//
auto GetNow() {
  auto now_sys = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now_sys.time_since_epoch())
      .count();
}

template <class Service>
class GrpcCallTag {
 public:
  virtual ~GrpcCallTag() {}

  // Calls the callback associated with this tag.
  virtual void OnCompleted(Service* service, bool ok) = 0;
};

template <class Service>
class UntypedCall {
 public:
  virtual ~UntypedCall() {}

  // The implementation of this method should use `service` to handle
  // an incoming request, and (perhaps asynchronously) send the
  // response.
  //
  // One reference on `this` is transferred to the callee, and the
  // callee is responsible for releasing it (typically via
  // `Call::SendResponse()`).
  //
  // `ok` is true if the request was received in a "regular event",
  // otherwise false.
  virtual seastar::future<> RequestReceived(Service* service, bool ok) = 0;

  // This method will be called either (i) when the server is notified
  // that the request has been canceled, or (ii) when the request completes
  // normally. The implementation should distinguish these cases by querying
  // the `grpc::ServerContext` associated with the request.
  virtual seastar::future<> RequestCancelled(Service* service, bool ok) = 0;

  // Associates a tag in a `::grpc::CompletionQueue` with a callback
  // for an incoming RPC.  An active Tag owns a reference on the corresponding
  // Call object.
  class Tag : public GrpcCallTag<Service> {
   public:
    // One enum value per supported callback.
    enum Callback { kRequestReceived, kResponseSent, kCancelled };

    Tag(UntypedCall* call, Callback cb) : call_(call), callback_(cb) {}

    // Calls the callback associated with this tag.
    //
    // The callback takes ownership of `this->call_`.
    void OnCompleted(Service* service, bool ok) override {
      switch (callback_) {
        case kRequestReceived:
          return call_->RequestReceived(service, ok);
        case kResponseSent:
          // No special handling needed apart from the Unref below.
          break;
        case kCancelled:
          return call_->RequestCancelled(service, ok);
      }
    }

   private:
    UntypedCall* const call_;  // `this` owns one reference.
    Callback callback_;
  };
};

// Represents a pending call with known request and response message
// types, and a known request-handling method.
template <class Service, class GrpcService, class RequestMessage,
          class ResponseMessage>
class Call : public UntypedCall<Service> {
 public:
  // Represents the generic signature of a generated
  // `GrpcService::RequestFoo()` method, where `Foo` is the name of an
  // RPC method.
  using EnqueueFunction = seastar::future<> (GrpcService::*)(
      ::grpc::ServerContextBase*, RequestMessage*,
      ::grpc::ServerAsyncResponseWriter<ResponseMessage>*,
      ::grpc::CompletionQueue*, ::grpc::ServerCompletionQueue*, void*);

  // Represents the generic signature of a `Service::HandleFoo()`
  // method, where `Foo` is the name of an RPC method.
  using HandleRequestFunction = seastar::future<> (Service::*)(
      Call<Service, GrpcService, RequestMessage, ResponseMessage>*);

  Call(HandleRequestFunction handle_request_function)
      : handle_request_function_(handle_request_function),
        responder_(&ctx_),
        start_time_(GetNow()) {}

  virtual ~Call() {}

  seastar::future<> RequestReceived(Service* service, bool ok) override {
    if (ok) {
      return (service->*handle_request_function_)(this);
    }
    return seastar::make_ready_future();
  }

  seastar::future<> SendResponse(::grpc::Status status) {
    return seastar::make_ready_future().then(
        [this, _status = std::move(status)] {
          responder_.Finish(response, _status, &response_sent_tag_);
          delete this;
          return seastar::make_ready_future();
        });
  }

  seastar::future<> RequestCancelled(Service* service, bool ok) override {
    if (ctx_.IsCancelled()) {
      if (cancel_callback_) {
        return cancel_callback_();
      }
    }
  }

  // Registers `callback` as the function that should be called if and when this
  // call is canceled by the client.
  void SetCancelCallback(std::function<seastar::future<>()> callback) {
    cancel_callback_ = std::move(callback);
  }

  // Clears any cancellation callback that has been registered for this call.
  void ClearCancelCallback() { cancel_callback_ = nullptr; }

  // Enqueues a new request for the given service on the given
  // completion queue, using the given `enqueue_function`.
  //
  // The request will be handled with the given
  // `handle_request_function`.
  static void EnqueueRequest(GrpcService* grpc_service,
                             ::grpc::ServerCompletionQueue* cq,
                             EnqueueFunction enqueue_function,
                             HandleRequestFunction handle_request_function,
                             bool supports_cancel) {
    auto call = new Call<Service, GrpcService, RequestMessage, ResponseMessage>(
        handle_request_function);
    if (supports_cancel) {
      call->RegisterCancellationHandler();
    }

    // Initial ref for call handed to grpc; released in Tag callback.
    (grpc_service->*enqueue_function)(&call->ctx_, &call->request,
                                      &call->responder_, cq, cq,
                                      &call->request_received_tag_);
  }

  // Enqueues a new request for the given service on the given
  // completion queue, using the given `method_id`.
  //
  // The request will be handled with the given
  // `handle_request_function`.
  static void EnqueueRequestForMethod(
      GrpcService* grpc_service, ::grpc::ServerCompletionQueue* cq,
      int method_id, HandleRequestFunction handle_request_function,
      bool supports_cancel) {
    auto call = new Call<Service, GrpcService, RequestMessage, ResponseMessage>(
        handle_request_function);
    if (supports_cancel) {
      call->RegisterCancellationHandler();
    }

    // Initial ref for call handed to grpc; released in Tag callback.
    grpc_service->RequestAsyncUnary(method_id, &call->ctx_, &call->request,
                                    &call->responder_, cq, cq,
                                    &call->request_received_tag_);
  }

  RequestMessage request;
  ResponseMessage response;

  const std::multimap<::grpc::string_ref, ::grpc::string_ref>& client_metadata()
      const {
    return ctx_.client_metadata();
  }

 private:
  // Creates a completion queue tag for handling cancellation by the client.
  // NOTE: This method must be called before this call is enqueued on a
  // completion queue.
  void RegisterCancellationHandler() {
    ctx_.AsyncNotifyWhenDone(&cancelled_tag_);
  }

  HandleRequestFunction handle_request_function_;
  ::grpc::ServerContext ctx_;
  ::grpc::ServerAsyncResponseWriter<ResponseMessage> responder_;

  // Used as void* completion markers from grpc to indicate different
  // events of interest for a Call.
  typedef typename UntypedCall<Service>::Tag Tag;
  Tag request_received_tag_{this, Tag::kRequestReceived};
  Tag response_sent_tag_{this, Tag::kResponseSent};
  Tag cancelled_tag_{this, Tag::kCancelled};

  seastar::shared_mutex lock_;
  std::function<seastar::future<>()> cancel_callback_;
  long start_time_;
};
#endif