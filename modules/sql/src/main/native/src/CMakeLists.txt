file(GLOB SOURCES "*.cpp")

add_library(sql ${SOURCES})

add_dependencies(sql JniHeader)

target_link_libraries(sql
    PUBLIC sql-api
)
