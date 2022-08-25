# Tsurugi SQL console

Tsurugi SQL console is a text based SQL client program.

This module provides the following program entry.

* [ScriptRunner] - Executes SQL script files.
  * program arguments
    * `[0]` - path to the script file
    * `[1]` - Tsurugi OLTP server end-point URI

Developers can build other SQL scripting tools using following interfaces and classes.

* interfaces
  * [Engine] - SQL scripting engine interface
  * [SqlProcessor] - Handles SQL operations, like start transaction, commit, or execute queries
  * [ResultProcessor] - Handles SQL result set
* classes
  * [BasicEngine] - A basic implementation of [Engine]
  * [BasicSqlProcessor] - A basic implementation of [SqlProcessor], which submits individual SQL operations to the Tsurugi OLTP server
  * [BasicResultProcessor] - Prints result sets as JSON lines style
  * [SqlParser] - Splits SQL script into individual SQL statements

[ScriptRunner]:src/main/java/com/tsurugidb/tsubakuro/console/ScriptRunner.java
[Engine]:src/main/java/com/tsurugidb/tsubakuro/console/executor/Engine.java
[SqlProcessor]:src/main/java/com/tsurugidb/tsubakuro/console/executor/SqlProcessor.java
[ResultProcessor]:src/main/java/com/tsurugidb/tsubakuro/console/executor/ResultProcessor.java
[BasicEngine]:src/main/java/com/tsurugidb/tsubakuro/console/executor/BasicEngine.java
[BasicSqlProcessor]:src/main/java/com/tsurugidb/tsubakuro/console/executor/BasicSqlProcessor.java
[BasicResultProcessor]:src/main/java/com/tsurugidb/tsubakuro/console/executor/BasicResultProcessor.java
[SqlParser]:src/main/java/com/tsurugidb/tsubakuro/console/parser/SqlParser.java

## Language

```bnf
<script> ::= <statement>+

<statement> ::= <statement-body> <statement-delimiter>
             | <special-statement>

<statement-body> ::= <SQL-statement>
                  |  <start-transaction>
                  |  <commit-statement>
                  |  <rollback-statement>
                  |  <call-statement>
                  |  /* empty statement */

<SQL-statement> ::= (any text until <statement-delimiter>)

<start-transaction-statement> ::= ( "START" | "BEGIN" ) ( "LONG" )? "TRANSACTION" <transaction-option>*

<transaction-option> ::= "READ" "ONLY"
                      |  "READ" "ONLY" "DEFERRABLE"
                      |  "READ" "WRITE"
                      |  "WRITE" "PRESERVE" <table-list>
                      |  "READ" "AREA" "INCLUDE" <table-list> ( "EXCLUDE" <table-list> )?
                      |  "READ" "AREA" "EXCLUDE" <table-list> ( "INCLUDE" <table-list> )?
                      |  "EXECUTE" ( "PRIOR" | "EXCLUDING" ) ( "DEFERRABLE" | "IMMEDIATE" )?
                      |  "AS" <name-or-string>

<table-list> ::= <name-or-string> ( "," <name-or-string> )*
              |  "(" ")"
              |  "(" <name-or-string> ( "," <name-or-string> )* ")"

<name-or-string> ::= <name>
                  |  <character-string-literal>

<read-area-option> ::= "INCLUDE" <table-list>
                    |  "EXCLUDE" <table-list>

<commit-statement> ::= "COMMIT"
                    |  "COMMIT" "WAIT" "FOR" <commit-status>

<commit-status> ::= "ACCEPTED"
                 |  "AVAILABLE"
                 |  "STORED"
                 |  "PROPAGATED"

<rollback-statement> ::= "ROLLBACK"

<call-statement> ::= "CALL" <identifier> "(" <call-parameter-list>? ")"

<call-parameter-list> :: <value> ( "," <value> )*

<value> ::= <literal>
         |  <name>

<literal> ::= <numeric-literal>
           |  <character-string-literal>
           |  <boolean-literal>
           |  "NULL"

<name> ::= <identifier> ( "." <identifier> )*

<special-statement> ::= "\EXIT"
                     |  "\HALT"
                     |  "\STATUS"
                     |  "\HELP"

<statement-delimiter> ::= ";"
                       |  EOF
```

## Implementation note

### Logger names and levels

* `com.tsurugidb.tsubakuro.console.ScriptRunner`
  * `ERROR` - print critical runtime error messages
  * `WARN` - print runtime error messages
  * `INFO` - program start/finish message
  * `DEBUG` - print program parameters
* `com.tsurugidb.tsubakuro.console.executor.BasicEngine`
  * `DEBUG` - print engine progress
* `com.tsurugidb.tsubakuro.console.executor.BasicSqlProcessor`
  * `DEBUG` - print actual SQL requests
* `com.tsurugidb.tsubakuro.console.executor.ExecutorUtil`
  * `WARN` - print warnings
* `com.tsurugidb.tsubakuro.console.parser.Segment`
  * `TRACE` - print each tokens
* `com.tsurugidb.tsubakuro.console.parser.SegmentAnalyzer`
  * `DEBUG` - print analyze target
  * `TRACE` - print analyze progress
