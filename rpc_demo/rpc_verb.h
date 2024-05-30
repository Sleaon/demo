#pragma once
#include <cstdint>

#ifndef RPCVERB_H_
#define RPCVERB_H_

namespace rpcserver {

enum class RpcVerb : uint64_t {
  kMessgingService_ClientID = 0,
  kRpcTest_Echo,
  kLast
};

}  // namespace rpcserver

#endif
