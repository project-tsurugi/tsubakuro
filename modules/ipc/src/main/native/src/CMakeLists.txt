file(GLOB SOURCES "*.cpp")

add_library(tsubakuro ${SOURCES})

target_link_libraries(tsubakuro
    PUBLIC tsubakuro-api
    PRIVATE Boost::system
    PRIVATE rt
)
