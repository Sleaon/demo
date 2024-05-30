SET(SRC_PATH ${ROOT_DIR}/rpc_demo)
find_package(Protobuf REQUIRED CONFIG)
file(GLOB PROTOS "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
foreach(PROTO ${PROTOS})
    get_filename_component(PROTO_WE ${PROTO} NAME_WE)
    list(APPEND PROTO_SRCS "${SRC_PATH}/${PROTO_WE}.pb.cc")
    execute_process(
        COMMAND /usr/local/bin/protoc 
        --cpp_out=${SRC_PATH}
        --proto_path=${SRC_PATH}
        ${PROTO}
        WORKING_DIRECTORY ${SRC_PATH}
        ERROR_VARIABLE PROTO_ERROR
        RESULT_VARIABLE PROTO_RESULT
    )
    if (${PROTO_RESULT} EQUAL 0) 
    else ()
        message (FATAL_ERROR "Fail to generate cpp of ${PROTO} : ${PROTO_ERROR}")
    endif()
endforeach()
# set(SERVER_SRC 
#     ${ROOT_DIR}/echo_server.cc
# )
# set(CLIENT_SRC 
#     ${ROOT_DIR}/echo_client.cc
# )

set(SEASTAR_SRC 
    ${SRC_PATH}/main.cc
    # ${SRC_PATH}/messaging_service.cc
)

# add_executable(rpc_test ${PROTO_SRCS} ${SEASTAR_SRC})
add_executable(rpc_test ${SEASTAR_SRC})
# target_include_directories(rpc_test PUBLIC . ${Protobuf_INCLUDE_DIRS})
# target_link_libraries(rpc_test protobuf::libprotobuf Seastar::seastar )