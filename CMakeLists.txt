cmake_minimum_required(VERSION 3.5)
project(seastartest C CXX)
SET(CMAKE_C_STANDARD 17)
SET(CMAKE_CXX_STANDARD 20)
SET(CMAKE_CXX_FLAGS "-Wall")
set(ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(ABSL_ENABLE_INSTALL ON)
add_subdirectory(third_party/seastar)
include(FetchContent)
FetchContent_Declare(
  gRPC
  GIT_REPOSITORY ssh://git@ssh.github.com:443/grpc/grpc.git 
  GIT_TAG        v1.59.2
)
set(FETCHCONTENT_QUIET OFF)
FetchContent_MakeAvailable(gRPC)


file(GLOB PROTOS "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
foreach(PROTO ${PROTOS})
    get_filename_component(PROTO_WE ${PROTO} NAME_WE)
    list(APPEND PROTO_SRCS "${ROOT_DIR}/${PROTO_WE}.pb.cc" "${ROOT_DIR}/${PROTO_WE}.grpc.pb.cc")
    execute_process(
        COMMAND /usr/local/bin/protoc 
        --plugin=protoc-gen-grpc=/usr/local/bin/grpc_cpp_plugin
        --cpp_out=${ROOT_DIR}
        --grpc_out=${ROOT_DIR}
        --proto_path=${ROOT_DIR}
        ${PROTO}
        WORKING_DIRECTORY ${ROOT_DIR}
        ERROR_VARIABLE PROTO_ERROR
        RESULT_VARIABLE PROTO_RESULT
    )
    if (${PROTO_RESULT} EQUAL 0) 
    else ()
        message (FATAL_ERROR "Fail to generate cpp of ${PROTO} : ${PROTO_ERROR}")
    endif()
endforeach()

set(SERVER_SRC 
    ${ROOT_DIR}/echo_server.cc
)
set(CLIENT_SRC 
    ${ROOT_DIR}/echo_client.cc
)

set(SEASTAR_SRC 
    ${ROOT_DIR}/echo_server_tf.cc
    ${ROOT_DIR}/seastar_engine.cc
)

# add_executable(demo_server ${SERVER_SRC} ${PROTO_SRCS})
# target_include_directories(demo_server PUBLIC .)
# target_link_libraries(demo_server grpc++)

# add_executable(demo_client ${CLIENT_SRC} ${PROTO_SRCS})
# target_include_directories(demo_client PUBLIC .)
# target_link_libraries(demo_client grpc++)

add_executable(demo_server_seastar main.cc ${SEASTAR_SRC} ${PROTO_SRCS})
target_include_directories(demo_server_seastar PUBLIC .)
target_link_libraries(demo_server_seastar Seastar::seastar grpc++)

add_executable(demo_client_seastar client_main.cc ${SEASTAR_SRC} ${PROTO_SRCS})
target_include_directories(demo_client_seastar PUBLIC .)
target_link_libraries(demo_client_seastar Seastar::seastar grpc++)