file(GLOB SOURCES "*.cpp")

add_library(sql ${SOURCES})

target_link_libraries(sql
    PUBLIC sql-api
    PRIVATE Boost::system
)
