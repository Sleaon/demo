#include "rpc_verb.h"

#include <functional>
#include <string>

namespace rpcserver {

RpcVerb::RpcVerb(std::string_view service_name, std::string_view method_name)
    : service_name_(service_name), method_name_(method_name) {
  std::hash<std::string> hash_gan{};
  auto service_hash = hash_gan(service_name_);
  auto method_hash = hash_gan(method_name_);
  id_ = service_hash ^
        (method_hash + 0x9e3779b9 + (service_hash << 6) + (service_hash >> 2));
  Verbs().emplace(id_, *this);
}
RpcVerb::RpcVerb(uint64_t id) {
  auto it = Verbs().find(id);
  if (it != Verbs().end()) {
    *this = it->second;
  }
}
RpcVerb::operator uint64_t() const { return id_ }

}  // namespace rpcserver