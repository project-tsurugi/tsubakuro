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

## Examples

### retrieves a mock ticket

```java
TicketProvider tickets = new JwtTicketProvider(new HttpTokenProvider("http://example.com/harinoki"));

Ticket ticket = tickets.issue("test1", "password1");
ticket = ticket.refresh(ticket, 10, TimeUnit.MINUTES);

Credential cred = ticket.toCredential(TokenKind.ACCESS);
```
