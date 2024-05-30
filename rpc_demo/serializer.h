#pragma once
#include <cstdint>
#include <string>
#ifndef SERIALIZER_H
#define SERIALIZER_H

#include <google/protobuf/message.h>

#include <boost/type.hpp>
#include <seastar/core/byteorder.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ipv4_address.hh>
#include <seastar/net/ipv6_address.hh>
#include <type_traits>

namespace rpcserver {

template <typename T>
struct Serializer;

template <typename T, typename Input>
requires std::is_integral_v<T>
inline T deserialize_integral(Input& input) {
  T data;
  input.read(reinterpret_cast<char*>(&data), sizeof(T));
  return le_to_cpu(data);
}

template <typename T, typename Output>
requires std::is_integral_v<T>
inline void serialize_integral(Output& output, T data) {
  data = cpu_to_le(data);
  output.write(reinterpret_cast<const char*>(&data), sizeof(T));
}

template <typename T>
struct IntegralSerializer {
  template <typename Input>
  static T read(Input& v) {
    return deserialize_integral<T>(v);
  }
  template <typename Output>
  static void write(Output& out, T v) {
    serialize_integral(out, v);
  }
  template <typename Input>
  static void skip(Input& v) {
    read(v);
  }
};

template <>
struct Serializer<bool> {
  template <typename Input>
  static bool read(Input& i) {
    return deserialize_integral<uint8_t>(i);
  }
  template <typename Output>
  static void write(Output& out, bool v) {
    serialize_integral(out, uint8_t(v));
  }
};
template <>
struct Serializer<int8_t> : public IntegralSerializer<int8_t> {};
template <>
struct Serializer<uint8_t> : public IntegralSerializer<uint8_t> {};
template <>
struct Serializer<int16_t> : public IntegralSerializer<int16_t> {};
template <>
struct Serializer<uint16_t> : public IntegralSerializer<uint16_t> {};
template <>
struct Serializer<int32_t> : public IntegralSerializer<int32_t> {};
template <>
struct Serializer<uint32_t> : public IntegralSerializer<uint32_t> {};
template <>
struct Serializer<int64_t> : public IntegralSerializer<int64_t> {};
template <>
struct Serializer<uint64_t> : public IntegralSerializer<uint64_t> {};

template <typename T, typename Output>
inline void serialize(Output& out, const T& v) {
  Serializer<T>::write(out, v);
};

template <typename T, typename Output>
inline void serialize(Output& out, const std::reference_wrapper<T> v) {
  Serializer<T>::write(out, v.get());
}

template <typename T, typename Input>
inline auto deserialize(Input& in, boost::type<T> t) {
  return Serializer<T>::read(in);
}

template <typename Output>
void safe_serialize_as_uint32(Output& out, uint64_t data) {
  if (data > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("Size is too big for serialization");
  }
  serialize(out, uint32_t(data));
}

template <typename T>
concept protobuf_t = std::is_base_of<::google::protobuf::Message, T>::value;

template <protobuf_t T, typename Input>
inline T deserialize_protobuf(Input& input) {
  auto size = deserialize<uint32_t>(input,boost::type<uint32_t>());
  auto ret = seastar::uninitialized_string(size);
  input.read(ret.data(), size);
  T msg;
  msg.ParseFromArray(ret.data(), size);
  return msg;
}

template <protobuf_t T, typename Output>
inline void serialize_protobuf(Output& output, T data) {
  std::string buf;
  data.SerializeToString(&buf);
  safe_serialize_as_uint32(output, buf.size());
  output.write(buf.c_str(), buf.size());
}

template <protobuf_t T>
struct Serializer<T> {
  template <typename Input>
  static T read(Input& intput) {
    return deserialize_protobuf<T>(intput);
  }
  template <typename Output>
  static void write(Output& out, T v) {
    serialize_protobuf(out, v);
  }
};
template <>
struct Serializer<std::string> {
  template <typename Input>
  static auto read(Input& i) {
    auto size = deserialize(i, boost::type<uint32_t>());
    auto ret = seastar::uninitialized_string(size);
    i.read(ret.data(), size);
    return std::string(ret);
  }
  template <typename Output>
  static void write(Output& out, std::string v) {
    safe_serialize_as_uint32(out, v.size());
    out.write(v.c_str(), v.size());
  }
};

typedef seastar::net::inet_address inet_address;
template <>
struct Serializer<inet_address> {
  template <typename Input>
  static inet_address read(Input& in) {
    auto sz = deserialize(in, boost::type<uint32_t>());
    if (sz == std::numeric_limits<uint32_t>::max()) {
      seastar::net::ipv6_address addr(deserialize(
          in, boost::type<seastar::net::ipv6_address::ipv6_bytes>()));
      return inet_address(addr);
    }
    return inet_address(in_addr(sz));
  }
  template <typename Output>
  static void write(Output& out, inet_address v) {
    auto& addr = v;
    if (addr.is_ipv6()) {
      serialize(out, std::numeric_limits<uint32_t>::max());
      auto bv = v.data();
      out.write(reinterpret_cast<const char*>(bv), v.size());
    } else {
      uint32_t ip = addr.as_ipv4_address().ip;
      // must write this little (or rather host) endian
      serialize(out, ip);
    }
  }
};

template<typename T>
constexpr bool can_serialize_fast() {
    return !std::is_same<T, bool>::value && std::is_integral<T>::value && (sizeof(T) == 1 || __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__);
}

template<bool Fast, typename T>
struct serialize_array_helper;

template<typename T>
struct serialize_array_helper<true, T> {
    template<typename Container, typename Output>
    static void doit(Output& out, const Container& v) {
        out.write(reinterpret_cast<const char*>(v.data()), v.size() * sizeof(T));
    }
};

template<typename T>
struct serialize_array_helper<false, T> {
    template<typename Container, typename Output>
    static void doit(Output& out, const Container& v) {
        for (auto&& e : v) {
            serialize(out, e);
        }
    }
};

template<typename T, typename Container, typename Output>
static inline void serialize_array(Output& out, const Container& v) {
    serialize_array_helper<can_serialize_fast<T>(), T>::doit(out, v);
}

template<typename Container>
struct container_traits;


template<typename T, typename... Args>
struct container_traits<std::unordered_set<T, Args...>> {
    struct back_emplacer {
        std::unordered_set<T, Args...>& c;
        back_emplacer(std::unordered_set<T, Args...>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace(std::move(v));
        }
    };
};

template<typename T>
struct container_traits<std::list<T>> {
    struct back_emplacer {
        std::list<T>& c;
        back_emplacer(std::list<T>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace_back(std::move(v));
        }
    };
    void resize(std::list<T>& c, size_t size) {
        c.resize(size);
    }
};

template<typename T>
struct container_traits<std::vector<T>> {
    struct back_emplacer {
        std::vector<T>& c;
        back_emplacer(std::vector<T>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace_back(std::move(v));
        }
    };
    void resize(std::vector<T>& c, size_t size) {
        c.resize(size);
    }
};


template<typename T, size_t N>
struct container_traits<std::array<T, N>> {
    struct back_emplacer {
        std::array<T, N>& c;
        size_t idx = 0;
        back_emplacer(std::array<T, N>& c_) : c(c_) {}
        void operator()(T&& v) {
            c[idx++] = std::move(v);
        }
    };
    void resize(std::array<T, N>& c, size_t size) {}
};

template<bool Fast, typename T>
struct deserialize_array_helper;

template<typename T>
struct deserialize_array_helper<true, T> {
    template<typename Input, typename Container>
    static void doit(Input& in, Container& v, size_t sz) {
        container_traits<Container> t;
        t.resize(v, sz);
        in.read(reinterpret_cast<char*>(v.data()), v.size() * sizeof(T));
    }
};

template<typename T>
struct deserialize_array_helper<false, T> {
    template<typename Input, typename Container>
    static void doit(Input& in, Container& v, size_t sz) {
        typename container_traits<Container>::back_emplacer be(v);
        while (sz--) {
            be(deserialize(in, boost::type<T>()));
        }
    }
};

template<typename T, typename Input, typename Container>
static inline void deserialize_array(Input& in, Container& v, size_t sz) {
    deserialize_array_helper<can_serialize_fast<T>(), T>::doit(in, v, sz);
}

template<size_t N, typename T>
struct Serializer<std::array<T, N>> {
    template<typename Input>
    static std::array<T, N> read(Input& in) {
        std::array<T, N> v;
        deserialize_array<T>(in, v, N);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const std::array<T, N>& v) {
        serialize_array<T>(out, v);
    }
};

}  // namespace rpcserver
#endif