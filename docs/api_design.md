# tsubakuro API design
2023.05.18
horikawa

この文書は、tsubakuroのAPIとAPIを処理するモジュールのdesignを記す。

## 登場する要素
### 主要要素
主要要素が対応しているDBの要素と主要要素が果たす役割は以下の通り。
* Session：DBサーバとの接続に対応、各種Client等にattachされ、そのAPI処理に必要となるDBサーバとの通信を行う。
* Transaction：DBのトランザクションに対応、SQL実行を要求する
* ResultSet：selectにより得られる結果セットに対応、tsubakuroを使うプログラム（以降、アプリ）に結果セットを提供する
* PreparedStatement：プリペアードステートメントに対応、PreparedStatementが提供する機能はない

### 補助要素
補助要素が果たす役割は以下の通り。
* SessionBuilder：Sessionを作成する
* SqlClient, BackupClient, AuthClient等：APIを介した処理要求をchannel経由でDBサーバに送り、その処理結果をchannelから受け取って要求元に返す
* RelationCursor：DBサーバからシリアライズされて送られてくる結果セットをデシリアライズして読み込む
* Placeholders, Parameters：プリペアードステートメントに関するSqlRequest.Placeholder型やSqlRequest.Parameter型のデータ（「要求メッセージ」の項参照）を作成するユーティリティー
* Metadata：ResultSetを介して受け渡されるRecordの情報（各columnのデータ型）を格納する
  note: 実装では、RelationMetadataにAPIが用意されており、異なるクラス名で使うためRelationMetadataを継承したResultSetMetadataが存在する
* Types：DBが扱う型に関する情報（javaの型とprotocol bufferで定義されている型情報の対応関係）を格納する
* StatementMetadata, TableMetaedata：SQLの実行計画やtableの情報をDBに問い合わせた結果として返される情報を格納する

### 下位要素
API処理モジュールが使用する下位要素が果たす役割は以下の通り。
* SqlService：処理要求メッセージをchannelから送信し、その結果をFutureとして要求元に返す。
* channel：DBサーバへの実行要求送信及び実行結果受信とResultSet受信を行う
（Designは、Channel Design Documentに記載）

## 概要
### 処理シーケンス
アプリがtsubakuroのAPIを使ってDBサーバにSQL処理を要求し、その結果を受け取るための操作シーケンスは以下の通り。
1) SessionBuilderにDBサーバを指定するURIを渡してSessionを作成する
2) SqlClientにSessionをattachする
3) SqlClientにトランザクション開始（createTransaction()）を要求してTransactionを作成する
4) TransactionにSQL実行要求を行う
  * selectの場合はSQL実行の結果としてResultSetが作成されるので、それを介してDBサーバから送られる結果セットを受け取った後、select操作の結果（処理の成否）を受け取る
  * insert, update, delete等、ResultSetを受け取らないSQLの場合は、その操作の結果（処理の成否）を受け取る
5) Transactionに終了要求（commit()）を行う

### プリペアードステートメント
プリペアードステートメントを使用する場合は、前項に示した操作シーケンスが以下となる（追加、または変更となる部分のみを記載）。
2-1) SqlClientにSQL文とPlaceholdersを与えてprepare()を行う。（前項の手順2)の後に実行）
4') 2-1の結果として受け取ったPreparedStatementとParametersを使って、TransactionからプリペアードステートメントによるSQL文実行要求を行う。（前項の手順4)の代わりに実行）

### 補足
#### 処理結果の受け取り
tsubakuroでは、DBサーバに対する処理要求の結果は、総てFutureを介した非同期受け取りとなる。
すなわち、ある処理要求（`Areq`）の結果として`Ares`のオブジェクトが返されるとすると、DBサーバにAreqを要求した結果としてアプリは`Future<Ares>`を受け取ることになる。
アプリは、`Ares`が必要になった時点で`Future<Ares>`からget()して`Ares`のオブジェクトを取り出す。

#### 要求メッセージ
DBサーバに送るメッセージは拡張性・保守性を考慮し、protocol buffersで定義する。
このメッセージの大半は、アプリが意識することはないが、以下のメッセージ要素についてはアプリが直接扱う必要がある。
* SqlRequest.TransactionOption：TransactionType、WritePreserve、ReadArea等を指定する。主にLong Transactionを作成する際に使用する。
* SqlRequest.Placeholder, SqlRequest.Parameter：PreparedStatementのパラメータに関する情報。
* SqlCommon.AtomType：DBが格納するデータ型に関する情報。Metadata, SqlRequest.Placeholder, SqlRequest.Parameterで使用する。

## API詳細
### SessionBuilder
#### SessionBuilder作成（コンストラクタ）
下記は総てstaticなmethod
* connect(String endpoint) -> SessionBuilder
* connect(URI endpoint) -> SessionBuilder

endpointは下記URLにより指定する
* ipc接続："ipc:DB名"
* tcp/ip接続："tcp://DBサーバ名（or ipアドレス）:ポート番号/"

#### Credential指定
* withCredential(Credential credential) -> SessionBuilder
セッション作成時に認証を行う場合の認証情報（credential）を指定する。

#### Session作成
* create() -> Session
* create(long timeout, TimeUnit unit) -> Session
* createAsync() -> Future\<Session\>

### SqlClient
#### SqlClient作成（コンストラクタ）
下記はstaticなmethod
* attach(Session session) -> SqlClient

#### Transaction作成
* createTransaction() -> Transaction
* createTransaction(SqlRequest.TransactionOption option)

optionには、protocol buffers（SqlRequest.TransactionOption）で定義されている情報を与える。

#### Prepare
* prepare(String source, SqlRequest.Placeholder... placeholders) -> FutureResponse\<PreparedStatement\>
* prepare(String source, Collection<? extends SqlRequest.Placeholder> placeholders) -> FutureResponse\<PreparedStatement\>

sourceにSQL文、placeholderの名前と型をplaceholdersに指定する。

#### 情報取得
* explain(String source) -> FutureResponse\<StatementMetadata\>
* explain(PreparedStatement statement, SqlRequest.Parameter... parameters) -> FutureResponse\<StatementMetadata\>
* explain(PreparedStatement statement, Collection<? extends SqlRequest.Parameter> parameters) -> FutureResponse\<StatementMetadata\>
* getTableMetadata(String tableName) -> FutureResponse\<TableMetadata\>

explain対象となるSQL文は、plain textか、PreparedStatementとparametersのセットで与える。これは、TransactionのexecuteStatement(), exexuteQuery()でも同様。

### Transaction
#### ステートメント実行
* executeStatement(String source) -> FutureResponse\<Void\>
* executeStatement(PreparedStatement statement, SqlRequest.Parameter... parameters) -> FutureResponse\<Void\>
* executeStatement(PreparedStatement statement, Collection<? extends SqlRequest.Parameter> parameters) -> FutureResponse\<Void\>

SQL実行でエラーとなった場合は、戻り値であるFutureResponse\<Void\>をget()したときに例外となる。

#### クエリ実行
* executeQuery(@Nonnull String source) -> FutureResponse\<ResultSet\>
* executeQuery(PreparedStatement statement, SqlRequest.Parameter... parameters) -> FutureResponse\<ResultSet\>
* executeQuery(PreparedStatement statement, SqlRequest.Parameter> parameters) -> FutureResponse\<ResultSet\>

SQL実行でエラーとなった場合の動作は、エラーの内容に応じて下記のどちらかとなる。
1) ResultSetを戻せないエラーの場合は、FutureResponse\<ResultSet\>をget()したときに例外となる。
2) ResultSetを送っている最中にエラーが発生した場合は、ResultSetからのデータ読み込み時（読み込み途中でclose()した場合はclose()操作時）に例外が発生する。

### ResultSet
#### メタデータ取得
* getMetadata() -> ResultSetMetadata

#### カーソル移動
* nextColumn() -> boolean
* nextRow() -> boolean

カーソルを次の読み込み位置に移動させる。
移動させた後のカーソルからデータ読み込みが可能な場合、trueが返る。
nextColumn()がfalseを返した場合は、rowの終わりを意味するので、nextRow()を行ってカーソルを次の行に移動させる。
nectRow()がfalseを返した場合は、ResultSetの終わりを意味する。

#### データ取得・確認
* fetchBooleanValue() -> boolean
* fetchInt4Value() -> int
* fetchInt8Value() -> long
* fetchFloat4Value() -> float
* fetchFloat8Value() -> double
* fetchDecimalValue() -> BigDecimal
* fetchCharacterValue() -> String
* fetchOctetValue() -> byte[]
* fetchBitValue() -> boolean[]
* fetchDateValue() -> LocalDate
* fetchTimeOfDayValue() -> LocalTime
* fetchTimePointValue() -> LocalDateTime
* fetchTimeOfDayWithTimeZoneValue() -> OffsetTime
* fetchTimePointWithTimeZoneValue() -> OffsetDateTime
* fetchDateTimeIntervalValue() -> DateTimeInterval

カーソルが指す位置にあるデータを、そのデータ型に応じて読み込む。同じカーソル位置からの複数回fetchはできない（例外となる）。
なお、データ型は予めメタデータにより特定した上で、その型に応じたfetchメソッドを使う必要がある。

#### データ確認
* isNull() -> boolean

カーソルが指す位置にあるデータが存在しない場合（nullの場合）にtrueを返す。
