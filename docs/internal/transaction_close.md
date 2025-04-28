# Transaction closeに関する状態遷移
2024.12.04
NT horikawa

## 定義
### 状態
* initial
* committed
* rollbacked
* toBeClosed
* toBeClosedWithCommit
* toBeClosedWithRollback
* closed

### 変数
* FutureResponse<Void> commitResult
`synchronized (this)` により排他アクセスを保証

### 状態と変数等
| 状態 | commitResult | commit要求 | rollback要求 | delayedClose登録 | Transaction close<br>completed |
| ---- | ---- | ---- | ---- | ---- | ---- |
| initial | null | false | false | false | false |
| committed | not null | true| false | false | false |
| rollbacked | null | false | true | false | false |
| toBeClosed | null | false | false | true | false |
| toBeClosedWithCommit | not null | true | false | true | false |
| toBeClosedWithRollback | null | false | true | true | false |
| closed | - | - | - | - | false |

`-`はdon't careを示す。

## 状態遷移
| 状態 | event<br>(API call) | 遷移先 | 備考 |
| ---- | ---- | ---- | ---- |
| initial | commit() | committed | commitを要求する
| initial | rollback() | rollbacked | rollbackを要求する
| initial | close() | toBeClosed | DelayedCloseをDisposerに登録する
| committed | commit() | - | ccommitを要求している旨の例外を投げる
| committed | rollback() | - | commitを要求している旨の例外を投げる
| committed | close() | closed | commitのレスポンスが返ってきている場合、<br>doClose()を実行する
| committed | close() | toBeClosedWithCommit | commitのレスポンスが返ってきていない場合、<br>DelayedCloseをDisposerに登録する
| rollbacked | commit() | - | rollbackを要求している旨の例外を投げる
| rollbacked | rollback() | - | 正常終了を返す
| rollbacked | close() | toBeClosedWithRollback | DelayedCloseをDisposerに登録する
| toBeClosed | commit() | - | closeされている旨の例外を投げる
| toBeClosed | rollback() | - | closeされている旨の例外を投げる
| toBeClosed | close() | - | do nothing
| toBeClosed | doClose() | closed | rollbackとdisposeを要求する
| toBeClosedWithCommit | commit() | - | closeされている旨の例外を投げる
| toBeClosedWithCommit | rollback() | - | closeされている旨の例外を投げる
| toBeClosedWithCommit | close() | - | do nothing
| toBeClosedWithCommit | doClose() | closed | commitResultをget()し、不成功ならdisposeを要求する
| toBeClosedWithRollback | commit() | - | closeされている旨の例外を投げる
| toBeClosedWithRollback | rollback() | - | closeされている旨の例外を投げる
| toBeClosedWithRollback | close() | - | do nothing
| toBeClosedWithRollback | doClose() | closed | disposeを要求する
| closed | commit() | - | closeされている旨の例外を投げる
| closed | rollback() | - | closeされている旨の例外を投げる
| closed | close() | - | do nothing

`-`は「状態遷移しない」を示す。
doClose()はDisposerに登録したDelayedCloseのみからcallされる。他はpublicメソッド。