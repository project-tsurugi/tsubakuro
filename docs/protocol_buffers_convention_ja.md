# protocol buffersのconventionについて
2022.08.22
horikawa

この文書は、protocol buffersによるメッセージ定義に関するconventionを記す。

## 前提
* protocol buffersによるメッセージはサービス毎に用意する。
* 各サービス毎に定義するメッセージは、request, responseとそれらに共通するcommonの３種類。commonは存在しない場合もある。
* 上記の他に、サービス全体に共通する診断メッセージ（diagnostics）がある。

## ファイル名と位置
本節では、パス名等に「サービスを処理するモジュール名」と「サービス名」を表す文字列使用する。

サービスを処理するモジュール名の例はjogasaki, tateyama
サービス名の例は、sql, datastore, auth, framework, core

### ファイル名
request, response, commonのメッセージを定義した各ファイル名は以下。
* request.proto
* response.proto
* common.proto

その他、必要に応じてstatus.proto等を配置しても良い。

### ファイル格納場所
tsubaluroでは、topディレクトリからの相対パスを下記とするディレクトリに置く。
* ${ROOT}/modules/proto/src/main/protos/サービスを処理するモジュール名/proto/サービス名/

ここで、${ROOT}はtsubakuroリポジトリのrootディレクトリ

### 診断メッセージ
診断メッセージについては、以下とする。
ファイル名：diagnostics.proto
ファイル格納場所：${ROOT}/modules/proto/src/main/protos/tateyama/proto

## package名
package名は以下とする。
* サービスを処理するモジュール名.proto.サービス名.[request|response|common]

## javaのpackageとouter_classname
### java_package
* com.tsurugidb.サービス名.proto

# outer_classname
* `サービス名`[Request|Response|Common]
outer_classnameは、上記をjava class名のconventionに合わせて最初の1文字をupper case文字としたcamel caseとする。
