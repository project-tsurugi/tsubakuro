file(GLOB SOURCES "*.cpp")

add_library(sql-test ${SOURCES})

target_include_directories(sql-test
  PRIVATE ../../../main/native/include
)

target_link_libraries(sql-test
    PUBLIC sql-test-api
    PRIVATE Boost::system
)
