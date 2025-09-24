# Jetty base directory for integration tests

## requirements

* Jetty 12

## setup

```sh
# cd /path/to/auth-http/conf/jetty.base
cp /path/to/harinoki.war webapps/harinoki.war
```

## start jetty

```sh
java -jar $JETTY_HOME/start.jar etc/jaas-login-service.xml
```

### check jetty

```sh
curl -u tsurugi:password http://localhost:8080/harinoki/issue
#> {"type":"ok","token":"...","message":null}
```

## run tests with integration test

```sh
# cd /path/to/tsubakuro
./gradlew :tsubakuro-auth-http:test \
    -Pharinoki.endpoint=http://localhost:8080/harinoki \
    -Pharinoki.login=tsurugi:password
```
