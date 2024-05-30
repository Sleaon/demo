SET(SRC_PATH ${ROOT_DIR}/table_demo)
set(SRC 
    # ${SRC_PATH}/messaging_service.cc
)

SET(SRC_TEST
    ${SRC_PATH}/inlineskiplist_test.cc
)

add_executable(table_demo main.cc)
target_include_directories(table_demo PUBLIC .)

add_executable(table_test unit_test.cc ${SRC_TEST})
target_link_libraries(table_test PUBLIC GTest::gtest_main)
