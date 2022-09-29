# Tsurugi Authentication over HTTP

This module provides a implementation of Tsurugi Authentication client that communicates "Harinoki" authentication service over HTTP(s).

This module provides the following interfaces and classes.

* interface
  * [TokenProvider] - Interface of Issue/Refresh/Verify JWT token
* classes
  * [HttpTokenProvider] - [TokenProvider] implementation that communicates with "Harinoki" authentication service over HTTP(s)
  * [JwtTicketProvider] - TicketProvider implementation using [TokenProvider]

[TokenProvider]:src/main/java/com/tsurugidb/tsubakuro/auth/http/TokenProvider.java
[HttpTokenProvider]:src/main/java/com/tsurugidb/tsubakuro/auth/http/HttpTokenProvider.java
[JwtTicketProvider]:src/main/java/com/tsurugidb/tsubakuro/auth/http/JwtTicketProvider.java

## Gradle artifact

```gradle
implementation "com.tsurugidb.tsubakuro:tsubakuro-auth-http:${tsubakuroVersion}"
```

## Integration tests

see [Jetty base directory for integration tests](config/jetty-base/README.md).

## Example

```java
TicketProvider provider = new JwtTicketProvider(
        new HttpTokenProvider("http://example.com/path/to/harinoki"));

// retrieve refresh token
Ticket ticket = provider.issue("test1", "password1");

// retrieve access token
ticket = provider.refresh(ticket, 10, TimeUnit.MINUTES);

// establish a OLTP session with access token
Session session = SessionBuilder.connect("...")
        .withCredential(ticket.toCredential(TokenKind.ACCESS))
        .create();

// ...

// update access token on the ticket
ticket = provider.refresh(ticket, 10, TimeUnit.MINUTES);
session.updateCredential(ticket.toCredential(TokenKind.ACCESS)).await();
```
