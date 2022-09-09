file(GLOB SOURCES "*.cpp")

add_library(tsubakuro ${SOURCES})

target_link_libraries(tsubakuro
    PUBLIC tsubakuro-api
    PRIVATE Boost::system
    PRIVATE rt
)

target_compile_definitions(tsubakuro
    PRIVATE
        TSUBAKURO_LIBRARY_VERSION=${TSUBAKURO_LIBRARY_VERSION}
)

set_target_properties(tsubakuro
    PROPERTIES
        INSTALL_RPATH "\$ORIGIN"
        INSTALL_RPATH_USE_LINK_PATH ON
)

install(
    TARGETS
        tsubakuro
    EXPORT
        ${export_name}
    LIBRARY
        DESTINATION native
        COMPONENT Runtime
)
