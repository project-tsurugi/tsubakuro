# generate JNI header file
set (_srcDir "${PROJECT_SOURCE_DIR}/../java")
set (_classes "${PROJECT_SOURCE_DIR}/../../../build/classes/java/main")
add_custom_target("JniHeader")
add_custom_command(
        TARGET JniHeader
        COMMAND ${Java_JAVAC_EXECUTABLE} -h ${PROJECT_SOURCE_DIR}/include
	-sourcepath ${_srcDir}
        -classpath ${_classes}
	-d ${_classes}
	${_srcDir}/com/nautilus_technologies/tsubakuro/impl/low/sql/LinkImpl.java
)
