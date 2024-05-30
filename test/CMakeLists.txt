SET(SRC_PATH ${ROOT_DIR}/test)
# set(SERVER_SRC 
#     ${ROOT_DIR}/echo_server.cc
# )
# set(CLIENT_SRC 
#     ${ROOT_DIR}/echo_client.cc
# )

set(TEST_SRC 
    ${SRC_PATH}/main.cc
    # ${SRC_PATH}/messaging_service.cc
)

# add_executable(rpc_test ${PROTO_SRCS} ${SEASTAR_SRC})
add_executable(test ${TEST_SRC})
# target_include_directories(rpc_test PUBLIC . ${Protobuf_INCLUDE_DIRS})
 target_link_libraries(test PRIVATE nlohmann_json::nlohmann_json)
