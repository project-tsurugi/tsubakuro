# client診断情報
NT horikawa
2023.02.09

javaのJMX機構によりtsubakuroの稼働状態を示す情報を取り出す仕組みを実装する。
本メモでは、その仕組みにより提供するtsubakuro稼働情報をリストアップする。

## 稼働情報源の分類
稼働状況の源は、リソースと通信状況の２種類。

### リソース
tsubakuroを使うアプリケーションが扱う要素。

#### セッション
リソース階層の最上位に位置する。

#### プリペアードステートメント
セッションに属する。

#### トランザクション
セッションに属する。

#### リザルトセット
トランザクションに属する。

### 通信状況
tsubakuroを使うアプリケーションは直接には扱わないが、tsubakuroがサーバに送信した実行要求に関する要素。
tsubakuroがサーバに実行要求を送信するときに作成され、サーバから実行要求の完了を示すレスポンスを受信すると消滅する。

#### ステートメント実行要求
リザルトセット転送を伴わない実行要求。

#### クエリ実行要求
リザルトセット転送を伴う実行要求。


## 出力する稼働情報
### リソース
#### セッション
* セッションID

#### トランザクション
* 属するセッション
* トランザクションID

#### プリペアードステートメント
* 属するセッション
* プリペアードステートメントID

#### リザルトセット
* 属する~~トランザクション~~ セッション（tsubakuroではリザルトセットもセッションが管理）
* リザルトセット名

### 通信状況
#### ステートメント実行要求
* リクエストメッセージ（protocol buffers）

#### クエリ実行要求
* リクエストメッセージ（protocol buffers）
* 状態（リザルトセット通信路未接続、リザルトセット通信路接続完了）
* リザルトセット名（リザルトセット通信路接続完了の場合）
* 送信済のリザルトセットデータ量、受信済のリザルトセットデータ量（注:第一版では取得できない）


## 出力方法（暫定）
ここでは、tsubakuroから稼働情報を取得して表示できることが確認できた方法を示す。
なお、稼働情報はJMXのMBeanを経由してテキスト形式で取得可能としているので、
稼働情報の取得は、ここで示した方法に限定されるものではない。

### tsubakuroを使うアプリケーションの起動
jvm（javaコマンド）の起動オプションに下記を加える。
```
-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
```

### 稼働情報の表示
tsubakuro-examplesのcom.tsurugidb.tsubakuro.examples.diagnostics.JMXClientを実行する。


## 実装
### MBeanと各種プロパティ名等
tsubakuroの稼働情報はInterface SessionInfoMBean（下記に全体）を実装したSessionInfo（抜粋）からStringとして取得する。
```
package com.tsurugidb.tsubakuro.diagnostic.common;

public interface SessionInfoMBean {
    String getSessionInfo();
}
```

```
package com.tsurugidb.tsubakuro.diagnostic.common;

public class SessionInfo implements SessionInfoMBean {
    public String getSessionInfo() {
        ...
    }
}
```
