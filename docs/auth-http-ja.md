# Harinoki 認証サービスクライアントについて

## 利用方法

* 以下のアーティファクトをGradleから参照
  * `com.tsurugidb.tsubakuro:tsubakuro-auth-http:${tsubakuroVersion}`

## 利用イメージ

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
session.updateAuthentication(ticket.toCredential(TokenKind.ACCESS)).await();
```

## 設定方法

```java
TicketProvider provider = new JwtTicketProvider(
        new HttpTokenProvider(
            URI.create("http://example.com/path/to/harinoki"),
            HttpClient.newBuilder()
                    .version(Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds(20))
                    .build()));
```

* 注意点
  * 認証関連はリクエストヘッダに直接記述しているため、 `authenticator` の設定は不要
