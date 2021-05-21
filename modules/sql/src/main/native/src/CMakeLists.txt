# generate JNI header file
set (_srcDir "${PROJECT_SOURCE_DIR}/../java")
set (_classes "${PROJECT_SOURCE_DIR}/../../../build/classes/java/main")
set (_stubDir "${PROJECT_SOURCE_DIR}/include")
add_custom_target("JniHeader")
add_custom_command(
        TARGET JniHeader
        COMMAND ${Java_JAVAC_EXECUTABLE} -h ${_stubDir} -verbose
	-sourcepath ${_srcDir}
        -classpath ${_classes}
	-d ${_classes}
	${_srcDir}/com/nautilus_technologies/tsubakuro/HelloJNI.java
)

file(GLOB SOURCES "*.cpp")

add_library(sql ${SOURCES})

add_dependencies(sql JniHeader)

target_link_libraries(sql
    PUBLIC sql-api
)
