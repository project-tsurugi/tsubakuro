# 「Tsubakuro TCP エンドポイントからのBLOB中継サービス利用」設計
2026.04.08

## この文書について
* この文書は「Tsubakuro TCP エンドポイントからの[BLOB中継サービス](https://github.com/project-tsurugi/tsurugi-issues/blob/draft-udf/drafts/internal/blob-relay-service_ja.md)」を保守するプログラマがsource codeの概要を把握する際に必要な情報を提供することを目的として、その設計を記述する。

## 設計方針
* tsubakuroとtsurugidb間でBLOBを転送する手段を選択可能とする
  * 転送手段の選択はtsubakuroがSessionを開設するときに行う
  * 選択可能な転送手段は、現時点では特権モードによるBLOB転送とBLOB中継サービスによるBLOB転送（gRPCのstreamingを使用）の２種だが、他の手段が用意された際の対応を容易とする仕組みとする
* アップロードは、BLOB転送クライアントを上位プログラムに提供し、上位プログラムが提供されたBLOB転送クライアントを使ってBLOB転送を行う方法を基本とする
  * 特権モードとBLOB中継サービスの差異は、上位プログラムに提供するBLOB転送クライアント実装の違いで吸収する
  * 従来API（特権モードによるBLOB転送用）もBLOB中継サービスによるBLOB転送でも使用可能とする

## 動作に登場する要素
### 登場人物（オブジェクト）とその役割
* `tateyama::endpoint`：`tateyama`のエンドポイント、`tsubakuro::Wire`と通信する
* `tsurugi::SQL`：`tateyama`のSQL実行エンジン、`tateyama::endpoint`からリクエスト・メッセージを受け取り、そのレスポンス・メッセージを返す
* `data-relay-grpc::blob_relay`：BLOB中継サービス、`tateyama`内の`統合gRPCサーバ`上で稼働する
* `tsubakuro::Session`：`tsubakuro`と`tateyama::endpoint`間の接続を統括する
* `tsubakuro::Sql`：`tsubakuro`のSQL処理部、`tsubakuro::Session`経由で`tsubakuro::Wire`を使い`tateyama::endpoint`と通信する
* `tsubakuro::Wire`：`tateyama::endpoint`と通信するクライアント部、`tsubakuro::Session`と1:1に対応する
* `tsubakuro::LargeObjectClient`：BLOBをアップロードするクライアントのインタフェース
  * 特権モード用とBLOB中継サービス用の実装がある
* `tsubakuroの上位プログラム`：`tsubakuro`のAPIを使用して`tsurugidb`にアクセスするクライアントプログラム
* `tateyama::blob_relay_privilege`：「特権モード」で受け渡されるBLOBファイルパスを管理するサービス
* `tsurugi::datastore`：ダウンロード対象BLOBファイルを格納する

### メッセージ（要求）や情報（知識）
#### メッセージ
* ハンドシェイク要求と応答
* プリペアドステートメント実行要求
* BLOB取得要求と応答
  * 特権モードでのみ使用
  * （memo: 既存実装ではGetLargeObjectDataリクエストとレスポンス）

#### 情報
* BLOB転送手段情報
  * 「`blob_relay`サービス」、「特権モード」、「BLOB転送を行わない」の何れかを示す情報
* BLOB中継サービス接続情報
  * 狭義には、`tsurugidb`内で稼働するgRPCサーバ上の`blob_relay`サービスに接続するための情報
    * `blob_relay`サービスの場合は以下を含める
      * 転送手段を示すシンボルを含める（memo: 現時点で使用可能な転送手段はstreamingのみ）
      * 転送手段に固有のパラメータ（memo: streamingの場合はendpoint URI, secure, stream_chunk_size）
  * 広義には、特権モードによるBLOB転送かBLOB中継サービスによるBLOB転送かを区別する情報を含む
* パラメータ
  * プリペアドステートメント実行要求に付与される変数名と値の組
  * 厳密には、パラメータのListがプリペアドステートメント実行要求に付与されるが、動作説明では（簡単のため）渡されるパラメータは１個として説明する
* カラム情報
  * SQL実行エンジンによるクエリ実行結果として返される`ResultSet`の要素であるカラムの値を示す情報
* column BLOB reference
  * selectの実行結果として返るカラムがBLOBの場合に得られるカラム情報
  * provider値とobject_id値とtag値の組
  * `data-relay-grpc::blob_relay`や`tateyama::blob_relay_privilege`に送られる際は`provider`値から`storage_id`値へのマッピングが行われる
  * `tsubakuro::Sql`が作成し、`data-relay-grpc::blob_relay`または`tateyama::blob_relay_privilege`が消費する
* BLOB relay reference
  * `blob_relay`サービスのアップロード操作により`data-relay-grpc::blob_relay`が返すstorage_id値とobject_id値とtag値の組
  * `data-relay-grpc::blob_relay`が作成し、`tateyama::endpoint`が消費する
* BLOBファイルパス
  * `tsubakuroの上位プログラム`が動作するマシンに格納されているBLOBファイルのpath情報
* upload BLOB情報
  * BLOB relay referenceまたはBLOBファイルパスを格納する
  * パラメータの値として利用可能

## 動作
本ドキュメントが対象としているBLOB転送に関係する動作は、Session確立時のハンドシェイク、BLOBのアップロード、BLOBのダウンロードの３種類である。CLOBについても基本動作は同じ。

### Session確立時のハンドシェイク
* `tsubakuroの上位プログラム`がBLOB転送手段情報を指定して`tsubakuro::Session`を作成する
  * 転送手段情報はRELAY, PRIVILEGED, DEFAULT, DOES_NOT_USEの何れか
    * `tateyama::endpoint`には`tsubakuroの上位プログラム`が使用可能なBLOB転送手段情報を送付
    * DEFAULTの場合は[RELAY, DOES_NOT_USE]というBLOB転送手段情報のリストを送付する
    * `tateyama::endpoint`はハンドシェイクの応答メッセージに使用するBLOB転送手段のBLOB中継サービス接続情報を含める
      * 使用するBLOB転送手段は、`tateyama::endpoint`が使用可能なBLOB転送手段の内、BLOB転送手段情報リストの最上位に位置するBLOB転送手段とする
    * BLOB転送手段情報リストにある転送手段が総て`tateyama::endpoint`で使用できない場合はハンドシェイク・エラーを返す
  * `tsubakuroの上位プログラム`はBLOB転送手段情報の知識を持っている
  * `tsubakuro::Session`はBLOB転送手段情報の知識を持つ
* `tsubakuro::Session`が`tateyama::endpoint`との接続を確立する処理で以下を行う
  * `tsubakuro::Session`は、BLOB転送手段情報を含むハンドシェイク要求を`tsubakuro::Wire`から`tateyama::endpoint`に送る
  * `tateyama::endpoint`は、BLOB転送手段情報がBLOB中継サービスの場合は、transaction idなしのBLOBセッションを作成する
  * `tateyama::endpoint`は、「BLOB 転送手段に関する情報」を`tsubakuro::Wire`経由で`tsubakuro::Session`に返す
    * これにより`tsubakuro::Session`はBLOB中継サービス接続情報の知識を持つ

#### BLOB 転送手段に関する情報について
* BLOB 中継サービスを利用する場合、以下の情報を含むBLOB中継サービス接続情報を返す
  * BLOB 中継サービスエンドポイント URI
  * セキュアチャネルか否か
  * 選択されたデータ転送路
    * データ転送路の名称 (e.g., stream)
  * 選択されたデータ転送路に対応するパラメータ (e.g., stream_chunk_size)
* 特権モードを利用する場合は、その旨を返す
* DOES_NOT_USEがBLOB転送手段情報のリストに含まれている場合は、RELAYやPRIVILEGEDが利用不可能でもハンドシェイクエラーにはせず、Sessionを作成する
  * この場合、`tateyama::endpoint`はBLOB 転送手段に関する情報は返さない（BLOB中継サービス接続情報は空で、特権モードは利用しないという情報を返す）
  * RELAYやPRIVILEGEDが利用不可能として作成されたSessionでBLOB操作を行うと`BlobException`が投げられる
* DOES_NOT_USEがBLOB転送手段情報のリストに含まれておらず、かつ、要求されたBLOB転送手段が利用不可能な場合はハンドシェイク・エラーを返し、セッションは作成しない

#### ハンドシェイクで指定するBLOB転送手段とサーバ設定による実行結果の関係
下表に示す。

| サーバ設定 | R=d, P=d | R=d, P=e | R=e, P=d | R=e, P=e |
| ---- | :----: | :----: | :----: | :----: |
| DOES_NOT_USE | N | N | N | N |
| PRIVILEGED | E | P | E | P |
| RELAY | E | E | R | R |
| DEFAULT | N | N | R | R |

サーバ設定行：`R`=Relay, `P`=Privileged, `e`=enabled, `d`=disabled  
結果：`N`=BLOB転送は使えない状態でセッションを開設、`E`=セッション開設エラー、`P`=PrivilegedでBLOB転送を行う、`R`=RelayでBLOB転送を行う

memo: サーバ設定欄の表記とtsurugi.iniの具体的な設定との関係は以下の通り
* `R=e`は`grpc_server.enabled=true`かつ`blob_relay.enabled=true`、`R=d`は左記以外
* `P=e`は`[ipc|stream]_endpoint.allow_blob_privileged=true`、`P=d`は`[ipc|stream]_endpoint.allow_blob_privileged=false`、ここで`[ipc|stream]`はセッション接続に使用する通信路。

### BLOBのアップロード
#### `tsubakuro::LargeObjectClient`を`tsubakuroの上位プログラム`が利用する場合のアップロード
* `tsubakuroの上位プログラム`は`tsubakuro::Session`から`tsubakuro::LargeObjectClient`を取得する
  * `tsubakuro::Session`は、使用するBLOB転送手段に応じた`tsubakuro::LargeObjectClient`を作成し、それを戻す
* `tsubakuroの上位プログラム`は`tsubakuro::LargeObjectClient`を使ってBLOBを処理し、その結果として`upload BLOB情報`を取得する
  * （ケース１）`blob_relay`サービスの場合
    * `tsubakuro::LargeObjectClient`は`data-relay-grpc::blob_relay`に対して`BlobRelayStreaming.Put()`によりBLOBをアップロードする
      * `context_id`は `tsubakuro::Session`が知識として持っている「BLOB 転送手段に関する情報」におけるBLOBセッションIDを使用する
    * その結果として受け取った`BLOB relay reference`から`upload BLOB情報`を作成し、`tsubakuroの上位プログラム`に戻す
  * （ケース２）「特権モード」の場合
    * `tsubakuro::LargeObjectClient`のPathを引数に取るuploadメソッドのみ使用可能、InputStreamやReaderを引数にとるuploadメソッドは使えない（`BlobException`が返る）
    * BLOBファイルパスである引数のPathに対してパスマッピングを実施する
    * 「特権モード」用の`tsubakuro::LargeObjectClient`は、アップロードでは`tateyama::endpoint`と通信しない
    * `tsubakuro::LargeObjectClient`はPathに対してパスマッピングを実施した後のパスから`upload BLOB情報`を作成し、`tsubakuroの上位プログラム`に戻す
* `tsubakuroの上位プログラム`は`tsubakuro::LargeObjectClient`から戻された`upload BLOB情報`を`com.tsurugidb.tsubakuro.sql.Parameters.blobOf()`に渡してパラメータを作成する
  * （memo）`upload BLOB情報`は、戻り値である`SqlRequest.Parameter`にある`SqlCommon.Blob`内の`oneof data`の`blob_info`に設定される
* `tsubakuroの上位プログラム`は`tsubakuro::Sql`にパラメータを含むプリペアドステートメント実行要求を行う
  * プリペアドステートメント実行要求を行うと、当該`upload BLOB情報`は使用できなくなる（使った場合の動作は不定）
* `tsubakuro::Sql`は`upload BLOB情報`を使うプリペアドステートメント実行要求を`tsubakuro::Session`を経由して`tsubakuro::Wire`に送る
* `tsubakuro::Wire`は、`upload BLOB情報`とプリペアドステートメント実行要求を`tateyama::endpoint`に送る

#### `tsubakuro::LargeObjectClient`を`tsubakuroの上位プログラム`が利用しない場合のアップロード
* `tsubakuroの上位プログラム`は`com.tsurugidb.tsubakuro.sql.Parameters.blobOf(String name, Path path)`にBLOBファイルパスを渡してパラメータを作成する
* `tsubakuroの上位プログラム`は`tsubakuro::Sql`に前記パラメータを含むプリペアドステートメント実行要求を行う
* `tsubakuro::Sql`は`tsubakuro::Session`から`tsubakuro::LargeObjectClient`を取得する
  * `tsubakuro::Session`は、使用するBLOB転送手段に応じた`tsubakuro::LargeObjectClient`を作成し、それを戻す
* `tsubakuro::Sql`は`tsubakuro::LargeObjectClient`を使ってBLOBを処理し、その結果として`upload BLOB情報`を取得する
* （ケース１）`blob_relay`サービスの場合
    * `tsubakuro::LargeObjectClient`は`data-relay-grpc::blob_relay`に対して`BlobRelayStreaming.Put()`によりBLOBをアップロードする
      * `context_id`は `tsubakuro::Session`が知識として持っている「BLOB 転送手段に関する情報」におけるBLOBセッションIDを使用する
    * `tsubakuro::LargeObjectClient`はPathに対してパスマッピングを実施した後のパスから`upload BLOB情報`を作成し、それを`tsubakuro::Sql`に戻す
* （ケース２）「特権モード」の場合
    * BLOBファイルパスである引数のPathに対してパスマッピングを実施する
    * 「特権モード」用の`tsubakuro::LargeObjectClient`は、アップロードでは`tateyama::endpoint`と通信しない
    * `tsubakuro::LargeObjectClient`はパスから`upload BLOB情報`を作成し、それを`tsubakuro::Sql`に戻す
* `tsubakuro::Sql`は`upload BLOB情報`を使うプリペアドステートメント実行要求を`tsubakuro::Session`を経由して`tsubakuro::Wire`に送る
* `tsubakuro::Wire`は、`upload BLOB情報`とプリペアドステートメント実行要求を`tateyama::endpoint`に送る

#### BLOB中継サービスでのアップロードにおける`data-relay-grpc::blob_relay`の動作（Put()）
* パラメータとして`context_id`を受け取る
* `context_id`に対応する`BLOB Session`の`session store`に、gRPCのストリーミング経由で送られるBLOBデータを格納する
* 前記ファイルを示す`BLOB relay reference`を`Put()`の戻り値としてgRPCの呼び出し元に戻す

#### 特権モードでのアップロードにおける`tateyama::blob_relay_privilege`
「特権モード」のアップロード操作ではblob_relay_privilegeは使用しない

#### アップロード時における`tateyama::endpoint`の動作
`tateyama::endpoint`は、`upload BLOB情報`が付与された実行要求メッセージを受け取ることがある。その場合、upload BLOB情報の種類に応じて以下の動作を行い、tateyama::api::server::requestがBLOBの問い合わせに対応できる状態にする
* BLOBファイルパス：ファイルパスをchannel_nameに関連付けて保存する
  * `tateyama::endpoint`に渡されるBLOBファイルパスはパスマッピング実施後のパス
* BLOB relay reference：tsurugiセッションに関連付けられているBLOBセッションに対して、storage_idとobject_idをキーとして対応するファイルパスを問い合わせる
  * ファイルパスが存在する場合は、tagの正当性を検証した後、そのファイルパスをchannel_nameに関連付けて保存する
* その後、実行要求メッセージを`tsurugi::SQL`に送る
これにより、`tsurugi::SQL`は`tateyama::endpoint`のリクエストオブジェクトからchannel_nameに対応するBLOBファイルのパスを取り出すことができる。


### BLOBのダウンロード
#### `tsubakuroの上位プログラム`によるBLOBのダウンロード
ダウンロードでは、`tsubakuroの上位プログラム`は`tsubakuro::Sql`の提供する従来APIを使う。
* `tsubakuroの上位プログラム`が`tsubakuro::Sql`からSELECT操作の結果として`column BLOB reference`を含むカラム情報を受け取る
* `tsubakuroの上位プログラム`は`column BLOB reference`を`tsubakuro::Sql`に渡してBLOBのデータ受け取りを要求する
* `tsubakuro::Sql`は`tsubakuro::Session`から`tsubakuro::LargeObjectClient`を取得する
  * `tsubakuro::Session`は、使用するBLOB転送手段に応じた`tsubakuro::LargeObjectClient`を作成し、それを戻す
* `tsubakuro::Sql`は`tsubakuro::LargeObjectClient`に、ダウンロード用の`contextId`として`transaction_handle`と`column BLOB reference`を渡してBLOB取得手段を呼び出す
  * （memo）`transaction_handle`は、アップロード時に使用する`blob_session_id`とは別の値である
  * この際、`tsubakuro::LargeObjectClient`は、１）指定された`contextId`が`transaction_handle`でない場合はエラーとする、２）column BLOB referenceの`provider`値を`data_relay_grpc.proto.blob_relay.blob_reference.BlobReference`の`storage_id`値にマッピングできない場合はエラーとする
    * （memo）現時点での`provider`と`storage_id`のマッピングは「表：`provider`と`storage_id`のマッピング」参照のこと
* `tsubakuro::LargeObjectClient`のBLOB取得手段は、転送手段に応じた動作（ケース１またはケース２）を行う
  * この際、`data_relay_grpc.proto.blob_relay.blob_reference.BlobReference`の`storage_id`値にはcolumn BLOB referenceの`provider`値をマッピングした値を設定する
  * （ケース１）`blob_relay`サービスの場合
    * `data-relay-grpc::blob_relay`に対して、`transaction_handle`と`column BLOB reference`の組をパラメータとして`BlobRelayStreaming.Get()`を呼び出してBLOBをダウンロードし、それをInputStream等`tsubakuroの上位プログラム`から要求された形態で`tsubakuro::Sql`に戻す
  * （ケース２）「特権モード」の場合
    * `tsubakuro::Session`と`tsubakuro::Wire`を経由して`tateyama::blob_relay_privilege`に`transaction_handle`と`column BLOB reference`の組をパラメータとしてGetBlob要求メッセージを送る
    * `tateyama::blob_relay_privilege`は`tsubakuro::Wire`にGetBlob応答メッセージを送る
      * GetBlob応答メッセージにはBLOBファイルパスが含まれる
    * `tsubakuro::LargeObjectClient`は、送られたBLOBファイルパスに対してパスマッピングを行う
    * `tsubakuro::LargeObjectClient`は、パスマッピングしたBLOBファイルパスで示されるファイルを読み込み、それをInputStream等`tsubakuroの上位プログラム`から要求された形態で`tsubakuro::Sql`に戻す
* `tsubakuro::Sql`は`tsubakuro::LargeObjectClient`から戻されたBLOBデータを`tsubakuroの上位プログラム`に戻す

表：`provider`と`storage_id`のマッピング
| `provider`| `storage_id` |
| ---- | ---- |
| DATASTORE<br>（値は`1`） | `1`<br>（LIMESTONE_BLOB_STORE） |

#### BLOB中継サービスでのダウンロードにおける`data-relay-grpc::blob_relay`の動作（Get()）
* パラメータとして`context_id`と`column BLOB reference`を受け取る
  * `context_id`はtsubakuro内部で作成し、`tsubakuroの上位プログラム`は作成や操作はしない
  * `column BLOB reference`はselect結果として`tsubakuroの上位プログラム`が`tsubakuro::Sql`から受け取るカラム情報
* 指定された `context_id` と BLOB参照の `storage_id` 情報をもとに、適切なストレージから `object_id` に該当する BLOB データを取り出す
  * その際、タグのチェックを行う
* BLOBファイルパスで示されるファイルを読み込み、gRPCのストリーミング経由でそのデータを`Get()`の呼び出し元に戻す

#### 特権モードでのダウンロードにおける`tateyama::blob_relay_privilege`の動作（GetBlob）
* パラメータとして`transaction_handle`と`column BLOB reference`を受け取る
* `transaction_handle`と`column BLOB reference`の`object_id`を使って`tsurugi::datastore`からダウンロード対象BLOBファイルのパスとtagを取得する
  * `column BLOB reference`のtagの正当性を確認する
  * `storage_id`が`tsurugi::datastore`を示す値であることを確認する、そうでない場合は、不正な`column BLOB reference`であるものとしてエラーを返す
* BLOBファイルパスをGetBlobの要求元に戻す


## 主要な変更
本項で示すクラスやインタフェースと本文の表記との対応関係を下表に示す。
| クラスやインタフェース | 本文の表記 |
| ---- | ---- |
| LargeObjectReference | 「情報」項のcolumn BLOB reference |
| LargeObjectClient | 「登場人物（オブジェクト）とその役割」項の`tsubakuro::LargeObjectClient` |
| LargeObjectInfo | 「情報」項のupload BLOB情報 |
| BlobRelayReference | 「情報」項のBLOB relay reference |

### LargeObjectReference追加（tsubakuro）
`tsubakuro::Session`に下記インタフェースを追加し、com.tsurugidb.tsubakuro.sql.LargeObjectReferenceはこのインタフェースを継承するように変更する。
```java
/**
 * An interface provided by a class that stores column information when a column in a result set returned by the SQL execution engine is a Large Object.
 */
public interface LargeObjectReference {
    /**
     * Returns the provider id of the Large Object data.
     * @return the provider id value
     */
    long getProvider();

    /**
     * Returns the object id of the Large Object data.
     * @return the object id value
     */
    long getObjectId();

    /**
     * Returns the reference tag of the Large Object data.
     * @return the reference tag value
     */
    long getReferenceTag();
}
```

### LargeObjectClient追加（tsubakuro）
`tsubakuro::Session`に下記インタフェースを追加する。
```java
/**
 * An abstract super interface of clients for handling Large Object upload and download.
 * This interface provides both upload APIs and download-related APIs
 * (e.g. opening streams/readers, cache access, and copy operations)
 * in a single client abstraction.
 *
 * @since 1.11.0
 */
public interface LargeObjectClient {
    /**
     * An interface representing the context of a Large Object download operation.
     * This is used to provide the transaction handle required for the download operation.
     */
    interface ContextId {
        /**
         * An enum representing the kind of context id.
         * This indicates that the context contains a transaction handle.
         */
        enum ContextIdKind {
          /**
           * Indicates that the context contains a transaction handle.
           * In this case, the transaction handle can be obtained by calling getTransactionHandle() method.
           */
          TRANSACTION,
        }

        /**
         * Returns the contextIdKind.
         * @return contextIdKind
         */
        ContextIdKind contextIdKind();

        /**
         * Returns the transaction handle.
         * @return the transaction handle, when {@link #contextIdKind()} is {@code TRANSACTION}
         * @throws IllegalStateException if no transaction handle is available for this context
         */
        long getTransactionHandle();
    }

    /**
     * Upload a Large Object passed from an InputStream.
     * @param source the InputStream through which the Large Object to be uploaded is passed
     * @return the LargeObjectInfo of the uploaded Large Object
     * @throws IOException if I/O error occurs while uploading Large Object
     * @throws BlobException if this instance is for privileged mode blob transfer
     */
    FutureResponse<LargeObjectInfo> upload(InputStream source) throws IOException, BlobException;

    /**
     * Upload a Large Object passed from a Reader.
     * @param source the Reader through which the Large Object to be uploaded is passed
     * @return the LargeObjectInfo of the uploaded Large Object
     * @throws IOException if I/O error occurs while uploading Large Object
     * @throws BlobException if this instance is for privileged mode blob transfer
     */
    FutureResponse<LargeObjectInfo> upload(Reader source) throws IOException, BlobException;

    /**
     * Upload a Large Object file.
     * The file cannot be deleted until the SQL execution that uses the <code>LargeObjectInfo</code>
     * returned by this method is complete, that is until the <code>FutureResponse</code> returned
     * by that SQL execution is retrieved via <code>get()</code> or <code>await()</code>.
     * In privileged mode, the file specified by the source parameter must be readable by the user running tsurugidb.
     * Otherwise, executing SQL using the uploaded Large Object will result in an error.
     * @param source the file path of the Large Object to be uploaded
     * @return the LargeObjectInfo of the uploaded Large Object
     * @throws IOException if I/O error occurs while uploading Large Object
     */
    FutureResponse<LargeObjectInfo> upload(Path source) throws IOException;

    /**
     * Returns an input stream for the Large Object.
     * @param contextId the contextId in the Large Object download operation
     * @param ref the large object reference
     * @return a future response of an InputStream of the Large Object
     * @throws IOException if I/O error occurs while sending request
     * @throws BlobException If the value of LargeObjectReference.getProvider() cannot be used for the download request
     */
    FutureResponse<InputStream> openInputStream(ContextId contextId, LargeObjectReference ref) throws IOException, BlobException;

    /**
     * Returns a reader for the Large Object.
     * @param contextId the contextId in the Large Object download operation
     * @param ref the large object reference
     * @return a future response of a Reader of the Large Object
     * @throws IOException if I/O error occurs while sending request
     * @throws BlobException If the value of LargeObjectReference.getProvider() cannot be used for the download request
     */
    FutureResponse<Reader> openReader(ContextId contextId, LargeObjectReference ref) throws IOException, BlobException;

    /**
     * Returns the LargeObjectCache for the Large Object.
     * The returned LargeObjectCache may be empty if blob_relay mode is used.
     * @param contextId the contextId in the Large Object download operation
     * @param ref the large object reference
     * @return a future response of LargeObjectCache
     * @throws IOException if I/O error occurs while sending request
     * @throws BlobException If the value of LargeObjectReference.getProvider() cannot be used for the download request
     */
    FutureResponse<LargeObjectCache> getLargeObjectCache(ContextId contextId, LargeObjectReference ref) throws IOException, BlobException;

    /**
     * Copy the large object to the file indicated by the given path.
     * <P>
     * If the destination file already exists, an IOException is thrown (for example,
     * FileAlreadyExistsException), and the BLOB data is not written.
     * If an error occurs while writing BLOB data, an IOException is thrown and the partially written 
     * file is deleted.
     * </P>
     * @param contextId the contextId in the BLOB/CLOB download operation
     * @param ref the large object reference
     * @param destination the path of the destination file
     * @return a future response of Void
     * @throws IOException if I/O error occurs while sending request
     * @throws BlobException If the value of LargeObjectReference.getProvider() cannot be used for the download request
     */
    FutureResponse<Void> copyTo(ContextId contextId, LargeObjectReference ref, Path destination) throws IOException, BlobException;
}
```
memo:
1.LargeObjectCacheは、`tsubakuro::Session`に作成する。
2.LargeObjectClientの具象クラスは下記情報を出力するtoStringメソッドを実装する
* 具象クラス名（特権モードかBLOB中継サービス利用モードかを区別するため）
* 接続先情報（BLOB中継サービスの場合、HandshakeレスポンスのBlobRelayServiceInfoの内容）

### LargeObjectInfoとBlobRelayReference追加（tsubakuro）
`tsubakuro::Session`に下記インタフェースとクラスを追加する。
``` java
/**
  * An interface representing the uploaded large object information.
  * <p>
  * If a prepared statement execution request is made, <code>LargeObjectInfo</code> becomes unavailable (behavior is undefined if used).
  * </p>
  */
public interface LargeObjectInfo {
    /**
     * The information type of uploaded large object.
     */
    enum InfoType {
          /**
           * Indicates that the information of the uploaded large object to the blob relay service.
           */
          BLOB_RELAY_REFERENCE,
          /**
           * Indicates that the information of the large object file, that is its file path as seen from the server.
           */
          SERVER_PATH,
        }

    /**
      * Returns the InfoType.
      * @return infoType
      */
    InfoType getInfoType();

    /**
     * Returns the BlobRelayReference.
     * @return the BlobRelayReference, when {@link #getInfoType()} is {@code BLOB_RELAY_REFERENCE}
     * @throws IllegalStateException when {@link #getInfoType()} is not {@code BLOB_RELAY_REFERENCE}
     */
    BlobRelayReference getBlobRelayReference();

    /**
     * Returns the Large Object file path string as seen from the server.
     * @return the Path representing the file path of the Large Object file, when {@link #getInfoType()} is {@code SERVER_PATH}
     * @throws IllegalStateException when {@link #getInfoType()} is not {@code SERVER_PATH}
     */
    String getServerPath();
}

/**
 * A class that stores reference of the BLOB/CLOB uploaded.
 */
public class BlobRelayReference {
    private final long storageId;
    private final long objectId;
    private final long tag;

    /**
     * Class constructor.
     * @param storageId the storage id value
     * @param objectId the object id value
     * @param tag the reference tag value
     */
    public BlobRelayReference(long storageId, long objectId, long tag) {
      this.storageId = storageId;
      this.objectId = objectId;
      this.tag = tag;
    }

    /**
     * Returns the storage id of the Large Object data.
     * @return the storage id value
     */
    public long getStorageId() {
      return storageId;
    }

    /**
     * Returns the object id of the Large Object data.
     * @return the object id value
     */
    public long getObjectId() {
      return objectId;
    }

    /**
     * Returns the reference tag of the Large Object data.
     * @return the reference tag value
     */
    public long getReferenceTag() {
      return tag;
    }
}
```

### `tateyama::blob_relay_privilege`追加（tateyama）
下記メッセージをやりとりする。
動作は「動作」「BLOBのダウンロード」「特権モードでのダウンロードにおける`tateyama::blob_relay_privilege`の動作（GetBlob）」項参照。
```protobuf
// the request message to blob_relay_privilege service.
message Request {
    // service message version (major)
    uint64 service_message_version_major = 1;

    // service message version (minor)
    uint64 service_message_version_minor = 2;

    // reserved for system use
    reserved 3 to 10;

    // the request command.
    oneof command {
        // Get blob operation.
        GetBlob get_blob = 11;
    }
}

// request of get blob operation.
message request.GetBlob {
    // context id
    oneof context_id {
        // the transaction handle.
        uint64 transaction_handle = 1;
    }

    // reserved for system use
    reserved 2 to 10;

    // the column BLOB reference.
    request.BlobReference blob_reference = 11;
}

message request.BlobReference {
    // the ID of the storage where the BLOB data is stored.
    uint64 storage_id = 1;

    // the ID of the object within the BLOB storage.
    uint64 object_id = 2;

    // a tag for additional access control.
    uint64 tag = 3;
}

// response for get blob operation.
message response.GetBlob {
    reserved 1 to 10;

    // request is successfully completed.
    message Success {
        // the blob file path as seen from the server.
        string server_file_path = 11;
    }
    ...
}
```
cf. 説明目的のため、protocol buffersの文法を逸脱している箇所がある。
コーディングの際は、適宜修正し、適切なファイルに配置すること。