# Tsurugi Authentication client mock

This module provides a mock implementation of [Tsurugi Authentication client](../auth/README.md).
It issues their own credentials, and they are incompatible with server-side auth mechanism; Please use this module only for testing.

This module provides the following class.

* [MockTicketProvider] - retrieves authentication information

[MockTicketProvider]:src/main/java/com/tsurugidb/tsubakuro/auth/mock/MockTicketProvider.java

## Examples

### retrieves a mock ticket

```java
TicketProvider tickets = new MockTicketProvider()
        .withUser("test1", "password1")
        .withUser("test2", "password2")
        .withRefreshExpiration(6, TimeUnit.HOURS)
        .withAccessExpiration(30, TimeUnit.MINUTES);

Ticket ticket = tickets.issue("test1", "password1");
ticket = ticket.refresh(ticket, 10, TimeUnit.MINUTES);

Credential cred = ticket.toCredential(TokenKind.ACCESS);
```

## Implementation Note

[MockTicketProvider] provides JWT based ticket, including the following claims:

| name | value |
|:-:|:-:|
| `issuer` | `mock` |
| `subject` | `refresh` or `access` |
| `audience` | user name |
| `issued_at` | issued date |
| `expires_at` | issued date + expiration time |

