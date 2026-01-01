cmake_minimum_required(VERSION 3.10)

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

project(tsubakuro-jni
    VERSION 0.1.0
    DESCRIPTION "Tsubakuro Native Wire Implementations"
    LANGUAGES C CXX)

option(BUILD_SHARED_LIBS "build shared libraries instead of static" ON)

include(GNUInstallDirs)

# find JNI, but AWT is not required
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    # no components are required, so give dummy optional-components
    find_package(JNI OPTIONAL_COMPONENTS JVM REQUIRED)
else()
    # hack: if the cache vars for AWT are filled, findJNI will not search AWT
    set(JAVA_AWT_LIBRARY NotNeeded)
    set(JAVA_AWT_INCLUDE_PATH NotNeeded)
    find_package(JNI REQUIRED)
endif()
message (STATUS "JNI_LIBRARIES: ${JNI_LIBRARIES}")

include_directories(${JNI_INCLUDE_DIRS})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")

find_package(Boost
    COMPONENTS system
    REQUIRED
)

if (NOT TSUBAKURO_LIBRARY_VERSION)
    set(
        TSUBAKURO_LIBRARY_VERSION ""
        CACHE STRING
        "native library version"
    )
endif (NOT TSUBAKURO_LIBRARY_VERSION)

set(export_name "tsubakuro")
set(package_name "tsubakuro")

include(CMakePackageConfigHelpers)

enable_testing()

add_subdirectory(src)

add_library(tsubakuro-api INTERFACE)

target_include_directories(tsubakuro-api
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/${export_name}>
)
