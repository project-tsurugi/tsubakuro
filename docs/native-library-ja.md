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

## 一貫性検証

* JNI ライブラリのロードに、Java ライブラリと JNI ライブラリのビルドの一貫性を検証し、不整合があればエラー終了する
  * ビルド時の環境変数 `GITHUB_SHA` を一貫性の識別子として利用する
* 上記のエラーを抑制するには、システムプロパティ `com.tsurugidb.tsubakuro.jniverify=false` を指定する
