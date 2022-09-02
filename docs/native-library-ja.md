# JNI ライブラリのロードについて

## 関連する設定項目

* システムプロパティ `com.tsurugidb.tsubakuro.jnilib`
  * プロパティに指定されたパス上の共有ライブラリをロードする
* 環境変数 `TSURUGI_HOME`
  * 環境変数に指定されたディレクトリ以下 `./lib/libtsubakuro.so` をロードする

## ロード順序

1. システムプロパティ `com.tsurugidb.tsubakuro.jnilib` に指定されたパス
2. 環境変数 `TSURUGI_HOME` 配下の `./lib/libtsubakuro.so`
3. ライブラリパスが通ったディレクトリ配下の `libtsubakuro.so`
