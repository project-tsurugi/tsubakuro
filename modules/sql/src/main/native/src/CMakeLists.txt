file(GLOB SOURCES "*.cpp")

add_library(wire ${SOURCES})

target_link_libraries(wire
    PUBLIC wire-api
    PRIVATE Boost::system
    PRIVATE rt
)
