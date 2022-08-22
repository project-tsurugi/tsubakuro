# protocol buffersのconventionについて
2022.08.22
horikawa

この文書は、protocol buffersによるメッセージ定義に関するconventionを記す。

## 前提
* protocol buffersによるメッセージはサービス毎に用意する。
* 各サービス毎に定義するメッセージは、request, responseとそれらに共通するcommonの３種類。commonは存在しない場合もある。
* 上記の他に、サービス全体に共通する診断メッセージ（diagnostics）がある。

## ファイル名と位置
### ファイル名
request, response, commonのメッセージを定義した各ファイル名は以下。
* request.proto
* response.proto
* common.proto

### ファイル格納場所
topディレクトリからの相対パスを下記とするディレクトリに置く。
* サービス名/

サービス名の例は、sql, datastore, auth, framework

top directoryの例は下記
* tateyama: ${ROOT}/src/tateyama/proto
* tsubakuro: ${ROOT}/modules/proto/src/main/protos/tateyama/proto
ここで、${ROOT}はリポジトリのrootディレクトリ

### 診断メッセージ
診断メッセージについては、以下とする。
ファイル名：diagnostics.proto
ファイル格納場所：topディレクトリ

## package名
package名は以下とする。
* tateyama.proto.[サービス名].[request|response|common]

## javaのpackageとouter_classname
### package
* com.tsurugidb.tateyama.proto

# outer_classname
* `サービス名`[Request|Response|Common]
outer_classnameは、上記をjava class名のconventionに合わせて最初の1文字をupper case文字としたcamel caseとする。
