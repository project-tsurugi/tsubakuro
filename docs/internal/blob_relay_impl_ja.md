# 「Tsubakuro TCP エンドポイントからのBLOB中継サービス利用」実装
2026.04.08

## この文書について
* この文書は[Tsubakuro TCP エンドポイントからのBLOB中継サービス利用](https://github.com/project-tsurugi/tsubakuro/blob/master/docs/blob_relay_design_ja.md)」を保守するプログラマがsource codeの概要を把握する際に必要な情報を提供することを目的として、その実装を記述する。

## 実装
### 概要
#### sql protocol buffersメッセージの追加・変更
* com.tsurugidb.sql.proto.SqlCommon.[Clob|Blob]
  * `oneof data`に`blob_info`を追加、local_pathと同様tsubakuro内部の情報伝達のために使用

#### endpoint protocol buffersメッセージの追加・変更
* `tateyama.proto.endpoint.request.Handshake`にBLOB中継サービスの種類を通知する優先順位付きリスト（`repeated BlobTransferMedium blob_transfer_media`）を追加する
* `tateyama.proto.endpoint.response.Handshake`にエンドポイント情報と`BLOB session`のIDを格納する`blob_relay_service_info`を追加する

service message versionを0.1（旧）から0.2（新）に上げる。
旧`tsubakuro`と新`tateyama::endpoint`の組み合わせは、特権モードによるBLOB転送が指定されたものとして扱う。
新`tsubakuro`と旧`tateyama::endpoint`の組み合わせは、`INVALID_REQUEST`エラーとなる。

#### framework header protocol buffersメッセージの追加・変更
* `tateyama.proto.framework.common.BlobInfo` の構造を変更する
  * 既存の `path` フィールドを、`oneof blob_location` の一要素として再構成する
* BLOB中継サービス向けの `BlobRelayReference` メッセージを`oneof blob_location` の一要素として追加する

service message versionを1.0（旧）から1.1（新）に上げる。
旧`tsubakuro`と新`tateyama::endpoint`の組み合わせは、正常に動作する（旧`tsubakuro`は特権モードによるBLOB転送のみ）。
新`tsubakuro`と旧`tateyama::endpoint`の組み合わせは、`INVALID_REQUEST`エラーとなる。

#### tsubakuro APIの変更・追加
* `com.tsurugidb.tsubakuro.common.SessionBuilder`に下記APIを追加
  * `withBlobTransfer(BlobTransferType type)`
    * このAPIを付与せずに`Session`を作成した場合は、`DEFAULT`が指定されたものとして扱う
* `com.tsurugidb.tsubakuro.common.Session`に下記APIを追加
  * `LargeObjectClient getLargeObjectClient()`
  * `BlobTransferMedium blobTransferMedium()`
* `com.tsurugidb.tsubakuro.common.BlobTransferType`を追加、`SessionBuilder.withBlobTransfer()`のパラメータとして指定するBLOB転送手段を示すenum
  * `DOES_NOT_USE`, `PRIVILEGED`, `RELAY`, `DEFAULT`の４種類
* `com.tsurugidb.tsubakuro.common.LargeObjectClient`インタフェースを追加
  * BLOBのアップロードとダウンロード手段を提供
* `com.tsurugidb.tsubakuro.sql.Parameters`に下記APIを追加、外部公開APIの受け入れ型は upload 結果全体を表す`LargeObjectInfo`に統一する
  * `public static SqlRequest.Parameter clobOf(String name, LargeObjectInfo info)`
  * `public static SqlRequest.Parameter blobOf(String name, LargeObjectInfo info)`
  * `SqlCommon.Clob` / `SqlCommon.Blob`および`BlobRelayReference`は公開APIの引数型にはせず、呼び出し側に組み立てさせない
  * これらは`Parameters`の内部で`LargeObjectInfo`から`reference`を取り出して生成する内部表現として扱う
* `com.tsurugidb.tsubakuro.sql.SqlClient`に下記APIを追加
  * `Session getSession()`
* `com.tsurugidb.tsubakuro.sql.LargeObjectReference`
  * Session層の`LargeObjectReference`を継承する

### 詳細
#### Session確立時のハンドシェイクのためのデータ構造への変更・追加
##### tsubakuro API
`tsubakuroの上位プログラム`が`BLOB中継サービス`を利用する場合、`SessionBuilder`に新設した`withBlobTransfer(BlobTransferType.RELAY)`（名前空間の上位は省略）により「TCPエンドポイントからのBLOB中継サービス利用」を設定してSessionを作成する。
```
Session session = SessionBuilder.connect(url).withBlobTransfer(BlobTransferType.RELAY).create();
```

##### protocol buffers（tateyama.proto.endpoint）
* `tateyama.proto.endpoint.request.Handshake`に複数の使用可能なBLOB中継サービス種類を通知するリスト（repeated BlobTransferMedium blob_transfer_media）を追加する
```protobuf
// handshake operation.
message Handshake {
    // the client information.
    ClientInformation client_information = 1;

    // prioritized candidates of BLOB transfer media/methods to use
    repeated BlobTransferMedium blob_transfer_media = 2;

    // reserved for system use
    reserved 3 to 10;

    // the wire information.
    WireInformation wire_information = 11;
}

// BLOB relay service type to use
enum BlobTransferType {
  // does not use BLOB transfer
  DOES_NOT_USE = 0;

  // privileged mode BLOB transfer
  PRIVILEGED = 1;

  // data_relay_grpc.proto.blob_relay_streaming.blob_relay
  RELAY = 2;
}

// BLOB relay service medium
message BlobTransferMedium {
  // BLOB relay service type
  BlobTransferType blob_transfer_type = 1;
}
```

* `tateyama.proto.endpoint.response.Handshake`にBLOB転送方式に応じた情報を返す`blob_transfer`を追加する
  * `oneof blob_transfer`のどのフィールドが設定されているかで、使用するBLOB転送方式が分かる
  * 特権モードによるBLOB転送を使用するときは`privileged_mode`フィールドを設定する
  * BLOB中継サービスによるBLOB転送を使用するときはエンドポイント情報を格納する`blob_relay_service_info`を設定する
  * BLOB転送を使用しないときは`blob_transfer`のいずれのフィールドも設定しない
```protobuf
    // request is successfully completed.
    message Success {
        // the session id.
        uint64 session_id = 11;

        // the user name.
        oneof user_name_opt {
            string user_name = 12;
        }

        // the BLOB transfer info
        oneof blob_transfer {
            // privileged mode is used
            Void privileged_mode = 13;

            // BLOB relay service is used, in this case, the service information is returned
            BlobRelayServiceInfo blob_relay_service_info = 14;
        }
    }


// empty message
message Void {}
```

* BlobRelayServiceInfoは下記
```protobuf
// the blob relay service information
message BlobRelayServiceInfo {
    // the Blob session id
    uint64 blob_session_id = 1;

    // the endpoint URI for BLOB relay service
    // Concretely, the string set at `grpc_server.endpoint` in `tsurugi.ini`
    string endpoint = 2;

    // the grpc server uses secure port
    // Concretely, the value of `grpc_server.secure` in `tsurugi.ini`
    bool secure = 3;

    // the name of transfer medium
    string medium = 4;

    // a map of parameter, a pair of key and value, for blob relay service
    map<string, string> parameters = 5;
}
```

#### アップロードのための変更・追加
##### com.tsurugidb.tsubakuro.common.Sessionクラス
下記メソッドを追加する。
```java
    /**
     * Returns a LargeObjectClient, a client for uploading and downloading BLOBs.
     * @return the LargeObjectClient for this session, which can be used as long as the session is alive
     * @throws IOException if an error occurs while initializing the LargeObjectClient
     * @throws IllegalStateException if the session does not handle BLOBs
     */
    LargeObjectClient getLargeObjectClient() throws IOException;

    /**
     * Returns the BlobTransferMedium that the LargeObjectClient uses.
     * @return the BlobTransferMedium
     */
    BlobTransferMedium blobTransferMedium();
```

##### com.tsurugidb.tsubakuro.common.BlobTransferTypeクラス
enumを新設する。
```java
package com.tsurugidb.tsubakuro.common;

import com.tsurugidb.endpoint.proto.EndpointRequest;

/**
 * Blob transfer type used in the session.
 */
public enum BlobTransferType {
    /**
     * Does not use transfer type.
     * An {@code EndpointRequest.BlobTransferMedium} entry whose type is
     * {@code EndpointRequest.BlobTransferType.DOES_NOT_USE} will be generated and sent to the endpoint
     */
    DOES_NOT_USE,

    /**
     * Privileged transfer type.
     * An {@code EndpointRequest.BlobTransferMedium} entry whose type is
     * {@code EndpointRequest.BlobTransferType.PRIVILEGED} will be generated and sent to the endpoint
     */
    PRIVILEGED,

    /**
     * Blob Relay transfer type.
     * An {@code EndpointRequest.BlobTransferMedium} entry whose type is
     * {@code EndpointRequest.BlobTransferType.RELAY} will be generated and sent to the endpoint
     */
    RELAY,

    /**
     * Indicates the default transfer policy.
     * A list of {@code EndpointRequest.BlobTransferMedium} entries will be generated and sent
     * to the endpoint in priority order: first
     * {@code EndpointRequest.BlobTransferType.RELAY}, then {@code EndpointRequest.BlobTransferType.DOES_NOT_USE}
     */
    DEFAULT;
}
```

##### com.tsurugidb.tsubakuro.common.BlobTransferMediumクラス
下記interface（classでも良さそう）を新設する。
```java
/**
 * Blob transfer medium used by the LargeObjectClient.
 */
public interface BlobTransferMedium {
    /**
     * Gets the BlobTransferType Blob transfer type used in the session.
     * @return the BlobTransferType other than BlobTransferType.DEFAULT
     */
    BlobTransferType getBlobTransferType();
}
```

##### com.tsurugidb.tsubakuro.common.LargeObjectClientインタフェース
「主要な変更」項記載のインタフェースを追加する。
各メソッドでタイムアウト処理を行う場合は、引数に`long timeout, TimeUnit unit`を加える。

##### com.tsurugidb.tsubakuro.sql.Parametersクラス
公開APIの受け入れ型は`LargeObjectInfo`に統一し、`Parameters`内部で`SqlCommon.Clob`/`SqlCommon.Blob`を生成する。下記メソッドを追加する。
```java
    /**
     * Returns a new {@code CLOB} parameter.
     * After using a returned SqlRequest.Parameter in a prepared statement execution request, 
     * the <code>reference</code> can no longer be used; behavior is undefined if used.
     * @param name the place-holder name
     * @param reference the info of the uploaded CLOB
     * @return the created place-holder
     *
     * @since 1.11.0
     */
    public static SqlRequest.Parameter clobOf(@Nonnull String name, @Nonnull LargeObjectInfo reference) {
        // 実装
    }
    /**
     * Returns a new {@code BLOB} parameter.
     * After using a returned SqlRequest.Parameter in a prepared statement execution request, 
     * the <code>reference</code> can no longer be used; behavior is undefined if used.
     * @param name the place-holder name
     * @param reference the info of the uploaded BLOB
     * @return the created place-holder
     *
     * @since 1.11.0
     */
    public static SqlRequest.Parameter blobOf(@Nonnull String name, @Nonnull LargeObjectInfo reference) {
            // 実装
    }
```

##### com.tsurugidb.tsubakuro.common.BlobInfoクラス
既存の`BlobInfo`で`BlobRelayReference`も扱えるように拡張する。
`BlobRelayReference`を扱う場合、`isFile()`は`false`、`getPath()`は空の`Optional.empty()`を返す。
```java
/**
 * An abstract super interface of BLOB data to send to Tsurugi server.
 *
 * @since 1.8.0
 */
public interface BlobInfo {

    /**
     * Returns the channel name for sending this BLOB data.
     * <p>
     * The channel name is used to identify the BLOB data in the server side,
     * so that it must be unique in the same request.
     * </p>
     * @return the channel name
     */
    String getChannelName();

    /**
     * Returns whether there is a file that represents this BLOB data in the local file system.
     * @return {@code true} if there is a file, otherwise {@code false}
     * @see #getPath()
     */
    boolean isFile();

    /**
     * Returns the path of the file as seen from the client that represents this BLOB data, only if it exists.
     * @return the path of the file when isFile() returns {@code true}, otherwise Optional.empty().
     * @see #isFile()
     */
    Optional<Path> getPath();

    /**
     * Returns the BlobRelayReference of the LargeObject uploaded to the BLOB relay service.
     * <p>
     * Used when uploading BLOBs via the BLOB relay service.
     * </p>
     * @return the BlobRelayReference of the LargeObject, or empty when the BLOB relay service is not used.
     *
     * @since 1.11.0
     */
    Optional<BlobRelayReference> getBlobRelayReference();
}
```

##### `com.tsurugidb.sql.proto.SqlRequest.Parameter`メッセージ
`oneof value`に`large_object_info_[clob|blob]`を追加し、BLOB中継サービスのBLOBを表すメッセージを新設する。
これらはtsubakuro内部の情報伝達のために使用する。
```protobuf
// the placeholder replacements.
message Parameter {
    // the placeholder location.
    oneof placement {
        // the placeholder name.
        string name = 2;
    }
    reserved 3 to 10;

    // the replacement values (unset describes NULL).
    oneof value {
（略）
        // the character large object.
        common.Clob clob = 31;

        // the binary large object.
        common.Blob blob = 32;

        // the ClientOnlyLargeObjectInfo for Large Object transfer (doesn't cross the network, only used in the local system).
        // Need to distinguish whether the parameter is a BLOB or a CLOB.
        // Since 1.11.0
        ClientOnlyLargeObjectInfo large_object_info_clob = 33;
        ClientOnlyLargeObjectInfo large_object_info_blob = 34;
（略）
    }

    reserved 12, 13, 20, 22, 24, 35 to 40, 41 to 50, 53 to 99;
}

// This is for LargeObject information handling within the client.
// This doesn't cross the network, and is only used in the local system.
message ClientOnlyLargeObjectInfo {
    oneof data {
        // the object value is stored in the local file.
        // for privileged mode
        string client_path = 1;

        // the object value is stored in the local file which can be accessed from the server through server_path.
        // for privileged mode
        string server_path = 2;

        // Results sent Large Object Data via the Blob relay service.
        // for blob relay service
        BlobRelayReference blob_relay_reference = 3;
    }
}

// Results sent Large Object Data via the Blob relay service.
message BlobRelayReference {
    // the ID of the storage where the BLOB data is stored.
    uint64 storage_id = 1;

    // the ID of the object within the BLOB storage.
    uint64 object_id = 2;

    // a tag for additional access control.
    uint64 tag = 3;
}
```

##### `com.tsurugidb.sql.proto.SqlCommon`メッセージ
tsubakuro内部情報伝達用の`local_path`は`SqlRequest.Parameter`の`large_object_info_[clob|blob]`に吸収し、`local_path`フィールドは廃止する。
```protobuf
// the character large object value.
message Clob {
    // reserved for deprecated field
    reserved 1;

    // the data of this large object.
    // SQL engine receives only the channel name, the other fields are for internal use within Tsubakuro.
    oneof data {
        // the channel name to transmit this object value.
        string channel_name = 2;

        // the immediate object value (for small objects).
        bytes contents = 3;
    }
}

// the binary large object value.
message Blob {
    // reserved for deprecated field
    reserved 1;

    // the data of this large object.
    // SQL engine receives only the channel name, the other fields are for internal use within Tsubakuro.
    oneof data {
        // the channel name to transmit this object value.
        string channel_name = 2;

        // the immediate object value (for small objects).
        bytes contents = 3;
    }
}
```

##### `tateyama.proto.framework.common.BlobInfo`メッセージ
```protobuf
// BLOB info.
message BlobInfo {

    // the channel name.
    string channel_name = 1;

    // information indicating the location of the BLOB file.
    oneof blob_location {
        // the file path as seen from the server.
        string path = 2;

        // the reference to the BLOB.
        BlobRelayReference blob = 4;
    }

    // whether file is temporary.
    bool temporary = 3;
}
```
`string path = 2;` であったフィールドを`oneof blob_location { string path = 2; BlobRelayReference blob = 4; }` に変更する。
なお、pathが設定されている旧型式メッセージを新形式メッセージ用の読み込みプログラムで扱うことは可能。

##### `tateyama.proto.framework.common.BlobRelayReference`メッセージ
下記メッセージを新設する
```protobuf
message BlobRelayReference {

    // the ID of the storage where the BLOB data is stored.
    uint64 storage_id = 1;

    // the ID of the object within the BLOB storage.
    uint64 object_id = 2;

    // a tag for additional access control.
    uint64 tag = 3;
}
```

##### 参考のため記載するメッセージ（変更なし）
`tateyama.proto.framework.request.Header`
```protobuf
// common request header for clients to set and send to tateyama.
message Header {
  // service message version (major)
  uint64 service_message_version_major = 1;

  // service message version (minor)
  uint64 service_message_version_minor = 2;

  // reserved for system use
  reserved 3 to 10;

  // the destination service ID.
  uint64 service_id = 11;

  // session ID.
  uint64 session_id = 12;

  // the BLOB info
  oneof blob_opt {
    common.RepeatedBlobInfo blobs = 13;
  }
}
```

`tateyama.proto.framework.common.RepeatedBlobInfo`（参考のため記載、メッセージは変更しない）
```protobuf
message RepeatedBlobInfo {
    repeated BlobInfo blobs = 1;
}
```

#### ダウンロードのための変更・追加
ダウンロードに関しては、上位プログラムが使う下記APIは変更しない。
* openInputStream(BlobReference ref) -> FutureResponse<InputStream>
* openReader(ClobReference ref) -> FutureResponse<Reader>
* getLargeObjectCache(LargeObjectReference ref) -> FutureResponse<LargeObjectCache>
* copyTo(LargeObjectReference ref, Path destination) -> FutureResponse<Void>

これらの上位APIのシグネチャは従来どおりとし、`ContextId`を引数として追加しない。
`RELAY`使用時に`LargeObjectClient`のダウンロードAPIが`ContextId`（transaction handleを含む）を必要とする場合は、`Transaction`等の既存API実装が橋渡し箇所となり、保持しているtransaction handleから`ContextId`を生成して`LargeObjectClient`に渡す。
すなわち、`openInputStream()`、`openReader()`、`getLargeObjectCache()`、`copyTo()`の各実装は、外部APIを変更せずに内部で`ContextId`を組み立てて`LargeObjectClient`へ委譲する。

`RELAY`使用時の`getLargeObjectCache(LargeObjectReference ref)`はキャッシュを表す`LargeObjectCache`を返し、その`find()`の結果は`Optional.empty()`となる。

#### `tsubakuro`への実装ポイント
##### Session確立時のハンドシェイク
* `tsubakuroの上位プログラム`が使用するBLOB転送手段を設定してSession（`tsubakuro`-`tateyama::endpoint`接続の意）を作成する
  * BLOB転送手段設定が行われていない場合（default）は、blob_relayを優先して使用する
  * defaultでは、`tsubakuro`はハンドシェイク時に候補として `[RELAY, DOES_NOT_USE]` を`tateyama::endpoint`に伝える
  * そのため、blob_relayが使用できない場合でも、`DOES_NOT_USE` が使用可能であればハンドシェイクは成功し、Sessionを開設する
* `tsubakuro`はハンドシェイク時に、設定されたBLOB転送手段、またはdefaultの場合はその候補順を`tateyama::endpoint`に伝える
* `tateyama::endpoint`は、ハンドシェイクで選択されたBLOB転送手段がblob_relayの場合、`BLOB session`（[BLOB中継サービス](https://github.com/project-tsurugi/tsurugi-issues/blob/draft-udf/drafts/internal/blob-relay-service_ja.md)参照）を作成し、BLOB転送に必要な情報をハンドシェイク結果の一部として`tsubakuro`に返す
  * `BLOB session`は、対応する`tsubakuro::Session`に紐づく`tateyama::endpoint`上のSessionが終了する際にクローズする
  * すなわち、`tsubakuro`の上位プログラムが`tsubakuro::Session.close()`等によりSession終了を要求すると、それが`tateyama::endpoint`のSession終了をトリガし、その結果として当該`BLOB session`が解放される
* `tsubakuro`は「BLOB転送に必要な情報」をSessionに紐づけて保存する
  * `tsubakuroの上位プログラム`や`tsubakuro`の`Sql`モジュールからの`LargeObjectClient`取得要求に応じて、保存しておいた「BLOB転送に必要な情報」を使って転送手段に応じた`LargeObjectClient`を作成して提供する
  * したがって、blob_relayを使用するSessionでは、`LargeObjectClient`利用後に最終的に`tsubakuro::Session.close()`等でSessionを終了させることが、サーバ側`BLOB session`を確実に解放する契機となる

`tsubakuro`は、明示的に指定されたBLOB転送手段が使用できない場合、またはdefaultで提示した候補のいずれも使用できない場合、ハンドシェイクをエラーとしてSessionを開設しない。

##### アップロード
アップロードはprepared statement（本ドキュメントではprepared queryを含める）のパラメータとして指定されたBLOBを`SQL実行エンジン`に届ける操作であり、下記に示す２つの内、いずれかの方法で行う。
* `tsubakuroの上位プログラム`が`tsubakuro`の`Parameters.blobOf(String name, Path path) -> SqlRequest.Parameter`でパラメータを指定し、`tsubakuro`がBLOBをアップロードする、具体的な手順は下記
  * `tsubakuro`は`BLOB中継サービス`を使う`Session`の場合は、`executePreparedStatement()`や`executePreparedQuery()`の処理で`Session`から`LargeObjectClient`を取得する
  * 取得した`LargeObjectClient`を使って、`path`で指定されるファイルをuploadし、その結果として`upload BLOB情報`を受け取る
  * 受け取った`upload BLOB情報`に含まれる`BlobRelayReference`の情報を`SqlCommon.Blob`に詰め替え、`tateyama::endpoint`に送るrequestに含める
* `tsubakuroの上位プログラム`がprepared statementの実行要求に先立って`Session`から`LargeObjectClient`を取得し、それを使ってBLOBをアップロードする、具体的な手順は下記
  * `tsubakuroの上位プログラム`がBLOBをSessionから取得した`LargeObjectClient`によってアップロードし、戻り値（`upload BLOB情報`）を受け取る
  * `tsubakuroの上位プログラム`はprepared statementまたはqueryの実行要求を行う際のパラメータ（`SqlRequest.Parameter`）を`Parameters.blobOf()`に前記戻り値（`upload BLOB情報`）を渡して作成する。その際、`upload BLOB情報`に含まれる`BlobRelayReference`の情報を`SqlCommon.[Clob|Blob]`に詰め替えてパラメータに格納したうえで`Transaction.executeStatement()`または`Transaction.executeQuery()`を呼び出す
  * `Transaction.executeStatement()`または`Transaction.executeQuery()`では、`SqlCommon.[Clob|Blob]`が指定されている`[blob|clob]`を持つパラメータが指定された場合、受け取った`SqlCommon.[Clob|Blob].blob_relay_reference`をもとに `tateyama.proto.framework.common.BlobRelayReference` を生成して `BlobInfo.blob_location.blob` に設定し、`SqlServiceStub.send(SqlRequest.ExecutePreparedStatement, blobs)`または`SqlServiceStub.send(SqlRequest.ExecutePreparedQuery, blobs)`を呼び出す
  * `SqlServiceStub.send()`は、`WireImpl.send(serviceId, payload, blobs)`をcallする
    * ここでserviceIdは`SQL実行エンジン`を示す値、payloadは`SqlRequest.ExecutePreparedStatement`または`ExecutePreparedQuery`をByteArrayにエンコーディングしたデータ
  * `WireImpl.send(serviceId, payload, blobs)`では、`getBlobRelayReference()`が有効な`SqlCommon.BlobRelayReference`を返す場合は、そこに格納されているobject_idとtagを `tateyama.proto.framework.common.BlobRelayReference`に設定し、それを`tateyama.proto.framework.request.Header.blob_opt` 内の `common.RepeatedBlobInfo.blobs` に追加する
  * `tateyama::endpoint` は、受信した `BlobRelayReference` の tag の正当性を検証したうえで、`channel_name` と `object_id` で示されるファイルパスおよび `temporary` フラグを `tateyama::api::server::blob_info` としてリクエストに紐づけて保持し、`tateyama::api::server::request::get_blob()` からの問い合わせに対して当該 `blob_info` を返す。

どちらの手段でも、`tateyama::endpoint`は以下の処理を行う
* 要求メッセージに`SqlCommon.Blob`が含まれている場合は、その情報からBLOBファイルの位置を調べ、requestに紐づく情報として保存しておく
* `SQL実行エンジン`からのBLOBについての問い合わせに対して、保存してあるBLOBファイル位置情報を返す

##### ダウンロード
ダウンロードはResultSetのcolumnとしてBLOBが指定された場合に行う。このResultSetのcolumnとしてBLOBが指定されたときの`tsubakuroの上位プログラム`動作は特権モードと同じ。
* 前段階
  * `SQL実行エンジン`はResultSetのcolumnがBLOBの場合、BLOBのcolumn情報としてstorage_id, object_id, reference_tagを`tsubakuro`に送る。ここでstorage_idは、`BlobReference` / `LargeObjectReference`でいうproviderに相当する。
  * `tsubakuro`は、送られたstorage_id（= provider）, object_id, reference_tagをBlobReference（厳密にはBlobReferenceForSql）に格納し、fetchBlob()の戻り値として`tsubakuroの上位プログラム`に渡す
    * providerとstorage_idのマッピングは「BLOBのダウンロード」「`tsubakuroの上位プログラム`によるBLOBのダウンロード」項参照のこと
* BLOB取得
  * `tsubakuroの上位プログラム`は、`BlobReference`を`tsubakuro.sql.Transaction.openInputStream()`, `openReader()`, `getLargeObjectCache()`, `copyTo()`に渡してBLOB取得を要求する
  * `tsubakuro`は、前述のAPIがcallされると、Session作成時に指定されたBLOBを転送手段に応じてBLOBを取得し、callされたAPIに応じた形態で`tsubakuroの上位プログラム`にBLOBのデータを提供する
    * 特権モードの場合は、従来通りの動作（`SQL実行エンジン`に問い合わせる）によりBLOBを取得する
    * `BLOB中継サービス`の場合は、`BlobReference`に設定されている情報により`BLOB中継サービス`のgRPC streamingメソッド（`BlobRelayStreaming.Get()`）を呼び出してBLOBを取得する

なお、clobについても基本動作は同じ（blobをclobに読み替える程度）。