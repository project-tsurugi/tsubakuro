# Requirements for and how to build Tsubakuro

## JDK
* OpenJDK 11 (see https://docs.microsoft.com/ja-jp/java/openjdk/download)
The environment variables JAVA_HOME and PATH should be as follows;
set JAVA_HOME to the directory where the JDK is installed,
and add the directory where the java command exists to PATH.

## How to build

### full build
Build java libraries(jar) and native shared libraries(so).

```
cd ${ProjectTopDirectory}
./gradlew build
```
where ${ProjectTopDirectory} is a directory created by clone of the tsubakuro repository in git (https://github.com/project-tsurugi/tsubakuro).


### build only java libraries
Build only java libraries and skip testing and building native libraries.

```
./gradlew assemble -x ipc:cmakeBuild
```

### install
Build and deploy the java and native libraries into Maven Local Repository.
```
./gradlew PublishToMavenLocal
```

### install only java libraries
Build and deploy only the java libraries into Maven Local Repository.
```
./gradlew PublishMavenJavaPublicationToMavenLocal
```

### generate all(aggregated) Javadoc
Generate Javadoc for whole Tsubakuro classes in directory `${ProjectTopDirectory}/build/docs/javadoc-all`.
```
cd ${ProjectTopDirectory}
./gradlew allJavadoc
```

Generated in directory `${ProjectTopDirectory}/build/docs/javadoc-all`

### generate Javadoc for client API
Generate Javadoc for Tsubakuro client API in directory `${ProjectTopDirectory}/build/docs/javadoc-client-api`.

```
cd ${ProjectTopDirectory}
./gradlew clientApiJavadoc
```
