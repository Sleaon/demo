#ifndef TENSORFLOW_CONTRIB_STAR_SEASTAR_SEASTAR_ENGINE_H_
#define TENSORFLOW_CONTRIB_STAR_SEASTAR_SEASTAR_ENGINE_H_

#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <map>
#include "options.h"
#include "seat"

class ServiceInterface;

class ServerFactory {
 public:
  // Creates a new server based on the given `server_def`, and stores
  // it in `*out_server`. Returns OK on success, otherwise returns an
  // error.
  virtual int NewServer(std::string_view service_name,
                           std::unique_ptr<ServiceInterface>* out_server) = 0;


  virtual ~ServerFactory() {}

  // For each `ServerFactory` subclass, an instance of that class must
  // be registered by calling this method.
  //
  // The `server_type` must be unique to the server factory.
  static void Register(std::string_view service_name, ServerFactory* factory);

  // Looks up a factory that can create a server based on the given
  // `server_def`, and stores it in `*out_factory`. Returns OK on
  // success, otherwise returns an error.
  static int GetFactory(std::string_view service_name,
                           ServerFactory** out_factory);
};

class SeastarEngine {
public:
  static SeastarEngine* GetInstance() {
    static SeastarEngine engine;
    return &engine;
  }
  void Init(const EngineOptions& options);
  int Start();

  virtual ~SeastarEngine();

  // seastar::co GetChannel();
private:
  SeastarEngine();
  SeastarEngine(const SeastarEngine&) = delete;
  SeastarEngine& operator =(const SeastarEngine&) = delete;

private:
  std::atomic<bool> _init_ready;
  size_t _core_number;
};

#endif // TENSORFLOW_CONTRIB_STAR_SEASTAR_SEASTAR_ENGINE_H_
