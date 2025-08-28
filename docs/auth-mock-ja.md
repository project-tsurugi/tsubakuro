# 認証APIのモック実装について

* コンセプト
  * スタンドアロンで動作する、認証APIのモック実装
    * [TicketProvider](https://github.com/project-tsurugi/tsubakuro/blob/f9c479310f164e2d3fd486e943b3839d8a456f3c/modules/session/src/main/java/com/nautilus_technologies/tsubakuro/low/auth/TicketProvider.java#L13) の mock として動作
  * 発行されるトークンに期限切れはあるが、認可機構と接続していないためサーバ側でトークンの検証は行われない
    * クライアント側で各種期限の確認は可能
  * AT/RT を分けて発行
* API 概要
  * `class MockTicketProvider implements TicketProvider`
    * `MockTicketProvider()`
      * インスタンスを生成する
    * `withUser(String user, String password)`
      * 指定のユーザ名とパスワードを認証可能エントリとして追加する (複数登録可)
    * `withRefreshExpiration(long time, TimeUnit unit)`
      * `issue()` によって発行する RT の有効期限を設定する (default: 6 hours)
      * `time<0` が指定された場合、 `issue()` は AT のみを発行する
    * `withAccessExpiration(long time, TimeUnit unit)`
      * `issue()` および `refresh()` によって発行する AT の有効期限を設定する (default: 10 minutes)
      * `time<0` が指定された場合、 `issue()` は RT のみを発行する
    * `withIssuedAt(Instant date)`
      * 各トークンの発行日を指定する (default: 発行タイミングごとに `Instant.now()`)
      * トークンの有効期限は発行日からのオフセットであり、 `issue()` や `refresh()` はここで指定した発行日をもとに有効期限を算出する
    * `@Override restore(String text) -> Ticket`
      * 文字列から RT または AT を復元する
    * `@Override issue(String userId, String password) -> Ticket`
      * `withUser()` によって登録された認証情報に合致した場合のみ、チケットを発行する
      * 発行されるチケットに含まれる RT, AT は `withIssuedAt()` によって設定された発行時刻と、 `with{Refresh,Access}Expiration()` によって指定された有効期限の影響を受ける
        * 発行時刻に有効期限の時間を足したものが、各トークンの有効期限となる
      * 該当する認証情報が登録されていない場合、 `CoreServiceException` がスローされ、コード [CoreServiceCode.AUTHENTICATION_ERROR](https://github.com/project-tsurugi/tsubakuro/blob/f9c479310f164e2d3fd486e943b3839d8a456f3c/modules/common/src/main/java/com/nautilus_technologies/tsubakuro/exception/CoreServiceCode.java#L53) が返される
    * `@Override refresh(Ticket, long expiration, TimeUnit unit) -> Ticket`
      * チケットに含まれる RT から AT を新たに発行する
        * 発行する AT の有効期限は、 `withIssuedAt()` によって指定された時刻に、引数の時間を足したものとなる
        * 指定可能な有効期限の範囲に特に制限はない (`withAccessExpiration()` は `issue()` のみに影響する)
      * RT が含まれていない、またはその有効期限が切れている場合、 `CoreServiceException` がスローされ、コード [CoreServiceCode.REFRESH_EXPIRED](https://github.com/project-tsurugi/tsubakuro/blob/f9c479310f164e2d3fd486e943b3839d8a456f3c/modules/common/src/main/java/com/nautilus_technologies/tsubakuro/exception/CoreServiceCode.java#L68) が返される
        * 有効期限のチェックは常に `Instant.now()` を用い、 `withIssuedAt()` に設定された時刻は利用しない
* 利用方法
  * 以下のアーティファクトをGradleから参照
    * `com.tsurugidb.tsubakuro:tsubakuro-auth-mock:${tsubakuroVersion}`
* 利用イメージ

  ```java
  TicketProvider tickets = new MockTicketProvider()
          .withUser("test1", "password1")
          .withUser("test2", "password2")
          .withRefreshExpiration(6, TimeUnit.HOURS)
          .withAccessExpiration(30, TimeUnit.MINUTES);

  Ticket ticket = tickets.issue("test1", "password1");
  // ticket = ticket.refresh(ticket, 10, TimeUnit.MINUTES);

  Session session = SessionBuilder.connect("...")
          .withCredential(ticket.toCredential(TokenKind.ACCESS))
          .create();

  // ...

  ticket = ticket.refresh(ticket, 10, TimeUnit.MINUTES);
  session.updateAuthentication(ticket.toCredential(TokenKind.ACCESS)).await();
  ```
