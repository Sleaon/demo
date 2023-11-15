project(seastartest C CXX)
SET(CMAKE_C_STANDARD 20)
SET(CMAKE_CXX_STANDARD 20)
SET(CMAKE_CXX_FLAGS "-Wall")
set(ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(../grpc)
add_subdirectory(../seastar)

aux_source_directory(. SRC)
add_executable(demo ${SRC})
target_include_directories(demo PUBLIC .)
target_link_libraries(demo Seastar::seastar gRPC::grpc++)