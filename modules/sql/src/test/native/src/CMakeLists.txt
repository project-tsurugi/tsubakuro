file(GLOB SOURCES "*.cpp")

add_library(wire-test ${SOURCES})

target_include_directories(wire-test
  PRIVATE ../../../main/native/include
)

target_link_libraries(wire-test
    PUBLIC wire-test-api
    PRIVATE Boost::system
    PRIVATE pthread
    PRIVATE rt
)
