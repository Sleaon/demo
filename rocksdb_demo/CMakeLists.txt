SET(SRC_PATH ${ROOT_DIR}/rocksdb_demo)
# set(SERVER_SRC 
#     ${ROOT_DIR}/echo_server.cc
# )
# set(CLIENT_SRC 
#     ${ROOT_DIR}/echo_client.cc
# )
find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h
    PATHS "")
include_directories(${ROCKSDB_INCLUDE_DIR})
find_library(ROCKSDB_LIB rocksdb)
message(STATUS "finding ${ROCKSDB_LIB}")
set(TEST_SRC 
    ${SRC_PATH}/main.cc
    # ${SRC_PATH}/messaging_service.cc
)

# add_executable(rpc_test ${PROTO_SRCS} ${SEASTAR_SRC})
add_executable(rocksdb_demo ${TEST_SRC})
# target_include_directories(rpc_test PUBLIC . ${Protobuf_INCLUDE_DIRS})
target_link_libraries(rocksdb_demo PRIVATE ${ROCKSDB_LIB})
