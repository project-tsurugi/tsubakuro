# 増分バックアップ関連API案

2022-11-01 arakawa (NT)

* 2022-11-17
  * group ID を廃止し、APIを整理
  * バックアップの種類を明記
  * 増分バックアップのコードイメージを追加
  * リストア時にバックアップファイルに相対パスを利用可能に
* 2022-11-23
  * API を整理

## この文書について

* Tsurugi OLTP に対する増分バックアップを実現するにあたり、クライアント側の利用方法のイメージを纏めたもの
  * 最終決定ではなく、検討のためのたたき台の段階

## バックアップ

### 基本的なデザイン - バックアップ

* 増分バックアップは、まず最初に基本となるフルバックアップ (ベースバックアップ) を作成し、そこに前回からの更新差分 (増分) を積み上げていく
  * 一連のバックアップを「シリーズ」とよぶ
  * 増分の計算は、ベースバックアップからではなく、シリーズの直前のバックアップから行う
* バックアップは Java API (Tsubakuro) を介して行う
  * Java API は対象のデータベースが稼働している必要があるため、未稼働の場合は別途起動させる
* フルバックアップでは単純なファイルリストであったが、増分では以下の情報を追加で利用する
  * ファイルの種類 (WAL, xLOB, metadata, etc.)
  * ファイルの配置パス (ファイルの識別子として利用)
  * 対象ファイルの移動可否
  * TBD: 高速化のため、フルバックアップにも一部の情報を輸入したほうがいいかも
* ベースバックアップは全てのファイルをバックアップ対象とするが、増分は一部のファイルのみを対象とする
  * 対象となるファイル種は以下
    * ログファイル
    * ログアーカイブ
    * ログメタデータ
    * xLOBファイル
    * xLOBメタデータ
  * 上記のファイル種は、API を通して判別可能である
* 増分を検出する上で、シリーズを通したファイルの一意性はファイルの配置パスを用いる
  * シリーズを通して同一 (identical) のファイルは、同一の配置パスが与えられる

### 提供するAPI - バックアップ

* [interface DatastoreClient](https://github.com/project-tsurugi/tsubakuro/blob/master/modules/session/src/main/java/com/tsurugidb/tsubakuro/datastore/DatastoreClient.java)
  * `beginBackup()`
    * overview
      * バックアップ操作を開始し、必要なファイルリストを受け取る
    * parameters
      * `type : BackupType` - バックアップの種類
        * フルバックアップであればすべてのファイルを対象にするが、増分のみを計算する場合は特定のファイル種だけを取得することになる
      * `label : String` - バックアップジョブのラベル (optional)
        * 追跡用
    * returns
      * `FutureResponse<BackupDetail>` - バックアップを行うための情報
* `enum BackupType`
  * `STANDARD` - 標準的なすべてのファイルをバックアップ対象とする (フルバックアップ向け)
  * `TRANSACTION` - ログと xLOB 関連のみをバックアップ対象とする (増分バックアップ向け)
* `interface BackupDetail`
  * `getConfigurationId()`
    * overview
      * バックアップの構成を表す識別子を返す
        * バックアップの構成が変更されると、増分を検出するための仕組みが大きく変わるため、フルバックアップの作成が必要になる
    * returns
      * `String` - 構成を表すID
    * note
      * 構成の変更は主に以下のケースを想定
        * データベースのバージョンアップ等でデータベース側のデータの持ち方が変わり、後方互換性が失われた
        * ユーザー設定によりデータベース側のファイルの配置が大きく変更され、差分の検出が困難になった
  * `nextEntries()`
    * overview
      * バックアップ対象のファイルリストの一部を返す
      * 繰り返し呼び出すことで、ファイルリストの全体を取り出せる
    * returns
      * list of `BackupDetail.Entry` - 次のファイルリストの一部
      * `null` - ファイルリストを末尾まで読みだした後
    * note
      * 単一のファイルセットに特定のファイル種がすべて含まれているとは限らない
        * 例えば、ログを2つのストレージに分けて保持している場合、少なくとも2つのファイルセットに分割される
  * `getLogBegin()`
    * overview
      * 利用可能なログの開始時刻を返す
    * returns
      * `long` - ログの開始時刻 (内部時刻のため、日付等にはマッピング不可)
    * note
      * 増分を作成する際、この値が前回の `getLogEnd()` 以下の値であることが要求される
        * 前回の `getLogEnd()` よりも大きな値である場合、一部のログが失われている
      * クリーンアップが行われると、この開始時刻が大きくなっていく
  * `getLogEnd()`
    * overview
      * 利用可能なログの終了時刻を返す
    * returns
      * `long` - ログの終了時刻 (内部時刻のため、日付等にはマッピング不可)
    * note
      * この値は主に増分を計算する際に、ログに抜けがないかを判断するためのものである
      * クラッシュしてログが壊れている場合、この情報を利用して末尾のトリムを行うかもしれない
  * `keepAlive()`
    * overview
      * API 呼び出しを行わないと一定時間でタイムアウトしてしまうため、このメソッドを通してタイムアウト時間の延長を行う
    * parameters
      * タイムアウトを延長する時間
  * `close()`
    * overview
      * バックアップ操作を完了させる
* `interface BackupDetail.Entry`
  * `getSourcePath()`
    * overview
      * バックアップ対象ファイルのデータベース上のパス (絶対パス) を返す
    * returns
      * `Path` - 対象のパス (絶対パス)
  * `getDestinationPath()`
    * overview
      * バックアップ対象ファイルの配置パスを返す
      * 同一のバックアップ対象ファイルに対し、シリーズを通して同一のファイルには同一の配置パスが与えられる
    * returns
      * `Path` - 対象の配置パス (相対パス)
  * `isMutable()`
    * overview
      * 対象ファイルが作成後、変更されうるかどうかを返す
    * returns
      * `true` - ファイルは変更されうるため、同一ファイルであっても差分検出が必要
      * `false` - ファイルは作成後変更されないため、同一ファイルであれば差分検出は不要
  * `isDetached()`
    * overview
      * 対象ファイルを移動してよいかどうかを返す
    * returns
      * `true` - 対象のファイルを移動してもよい
      * `false` - 対象のファイルを移動してはならず、コピーしなければならない

### イメージ - フルバックアップ

```java
// 現在のセッション
Session session = ...;
// バックアップ作成先
Path destination = ...;

try (
    // ログデータストアにアクセスするクライアントを作成する
    var client = DatastoreClient.attach(session);
    // バックアップを開始する (フルバックアップなので標準的なすべてのファイルを対象にする)
    var backup = client.beginBackup(BackupType.STANDARD).await();
) {
    while (true) {
        // 次のファイルセットを取り出す
        var block = backup.nextFileSet();

        // 末尾まで読んだら完了
        if (block == null) {
            break;
        }

        // 全てのファイルをバックアップする
        for (var entry : block.getEntries()) {
            // コピー元
            var from = entry.getSourcePath();
            // コピー先
            var to = destination.resolve(entry.getDestinationPath());

            if (entry.isDetached()) {
                // ... ファイルを移動
            } else {
                // ... ファイルをコピー
            }
        }

        // 定期的に接続を確保する
        backup.keepAlive();
    }
} // try-with-resources の末尾でバックアップは自動的に終了
```

### イメージ - 増分バックアップ

```java
// 現在のセッション
Session session = ...;
// バックアップ作成先
Path destination = ...;

// 直前の世代までに存在したファイルの配置パス一覧
Set<Path> lastFiles = ...;

// 今世代のファイル一覧
Set<Path> currentFiles = new HashSet<>();

try (
    // ログデータストアにアクセスするクライアントを作成する
    var client = DatastoreClient.attach(session);
    // バックアップを開始する (増分バックアップではログ(とxLOB)を対象にする)
    var backup = client.beginBackup(BackupType.TRANSACTION).await();
) {
    // ... 構成IDに変更がないかチェック
    // ... ログに分断がないかチェック

    while (true) {
        // 次のファイルセットを取り出す
        var block = backup.nextFileSet();

        // 末尾まで読んだら完了
        if (block == null) {
            break;
        }

        // 全てのファイルをバックアップする
        for (var entry : block.getEntries()) {
            // 今世代のファイル一覧を作成
            currentFiles.add(entry.getDestinationPath());

            // 差分検出
            if (lastFiles.contains(to) && !entry.isMutable()) {
                // 変更がないものは増分バックアップの対象外
                continue;
            }

            // ... フルバックアップ同様にコピー
        }

        // 定期的に接続を確保する
        backup.keepAlive();
    }

    // 今世代で削除されたファイルの一覧を取得
    var removedFiles = currentFiles.stream()
        .filter(lastFiles::contains)
        .collect(Collectors.toSet());

    // ... 今世代に関する情報を記録

} // try-with-resources の末尾でバックアップは自動的に終了
```

## リストア

### 基本的なデザイン - リストア

* 通常のフルバックアップと同様に [oltp restore backup コマンド](https://github.com/project-tsurugi/tateyama/blob/master/docs/cli-spec-ja.md#restore-%E3%82%B5%E3%83%96%E3%82%B3%E3%83%9E%E3%83%B3%E3%83%89) を利用してリストア操作を行う
  * ただし、フルバックアップからのリストアは単にディレクトリを指定していただけなのに対し、増分バックアップでは必要な個々のファイルを指定することになる
  * リストアに指定する個々のファイルは JSON で指定する
    * 前述の `BackupDetail` が返すデータ構造とほとんど同じ (一部不要なプロパティがない)
  * コマンドの引数にディレクトリを指定するか、ファイルリストを指定するかはコマンドパラメータに明記する (--use-file-list)
* 増分バックアップからリストアを行う際、シリーズから必要なファイルのみを列挙して、ファイルリストを作ることになる
  * 世代ごとに追加、変更、削除されたファイルを計算し、ファイルリストにまとめる

### ファイルリストのスキーマ - リストア

* `<root>`
  * `entries` - array of `file_set_entry`
* `object file_set_entry`
  * `source_path` - `string`
    * overview
      * 対象ファイルのパス
    * note
      * 相対パスが指定された場合、ファイルリストを格納するディレクトリからの相対パスとして計算される
      * セキュリティの観点から、相対パスに上位ディレクトリ (`..`) は含められない
  * `destination_path` - `string`
    * overview
      * 対象ファイルの配置パス
    * note
      * バックアップ時に指定された `FileSet.Entry.getDestinationPath()` の値を指定する
  * `detached` - `boolean`
    * overview
      * 対象ファイルを移動してよいかどうか
    * note
      * `true` が指定された場合、コマンドはこのファイルを移動するかもしれないし、しないかもしれない
