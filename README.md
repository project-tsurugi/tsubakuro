# Requirements for and how to build Tsubakuro

## Requirements

### JDK
* OpenJDK 11 (see https://docs.microsoft.com/ja-jp/java/openjdk/download)
The environment variables JAVA_HOME and PATH should be as follows;
set JAVA_HOME to the directory where the JDK is installed,
and add the directory where the java command exists to PATH.

### Dependency packages for Native Library
Tsubakuro needs to install several packages for Native Libarary builds.
See *Dockerfile* section.

### Dockerfile

```dockerfile
FROM ubuntu:20.04

RUN apt update -y && apt install -y git build-essential cmake libboost-system-dev openjdk-11-jdk
```

###

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
./gradlew assemble -x tsubakuro-ipc:nativeLib
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

## How to use

### Setup dependencies
To use on Gradle, add Tsubakuro libraries and SLF4J implementation library to `dependencies`.

```
dependencies {
    api 'com.tsurugidb.tsubakuro:tsubakuro-session:0.0.1'
    api 'com.tsurugidb.tsubakuro:tsubakuro-connector:0.0.1'

    implementation 'org.slf4j:slf4j-simple:1.7.32'
}
```

A list of available libraries (java artifacts) can be found at:
* https://github.com/orgs/project-tsurugi/packages?repo_name=tsubakuro

### Maven Repository Configuration
The java artifacts in this repository are distributed on GitHub Packages,.

To use these artifacts, add a Maven remote repository for GitHub Packages or a `mavenLocal` to `repositories` configuration in `build.gradle`.

Please see the following `build.gradle` examples.
* https://github.com/project-tsurugi/iceaxe/blob/master/buildSrc/src/main/groovy/iceaxe.java-conventions.gradle

#### Use Tsubakuro that deployed GitHub Packages

Set up the following credentials for the GitHub Packages, and build nomally.
* Gradle property `gpr.user` or environment variable `GPR_USER` with your GitHub username
* Gradle Property `gpr.key` or environment variable `GPR_KEY` with your personal access token

For more information about personal access token, please see the following documents.
* https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-gradle-registry


#### Use Tsubakuro that installed locally

First, Install Tsubakuro locally following the steps in `How to Build`, and build project with Gradle Property `mavenLocal` option.

```
./gradlew build -PmavenLocal
```
