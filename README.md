# Requirements for and how to build Tsubakuro

## JDK
* OpenJDK 11 (see https://docs.microsoft.com/ja-jp/java/openjdk/download)
The environment variables JAVA_HOME and PATH should be as follows;
set JAVA_HOME to the directory where the JDK is installed, 
and add the directory where the java command exists to PATH.

## How to build
```
cd ${ProjectTopDirectory}
./gradlew biuld
```
where ${ProjectTopDirectory} is a directory created by clone of the tsubakuro repository in git (https://github.com/project-tsurugi/tsubakuro).
