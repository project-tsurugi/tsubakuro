# Tsurugi execution plan information utilities

This module provides utilities to parse execution plan information obtained by `SqlClient.explain()` operation.

This includes the following interfaces and classes:

* [JsonPlanGraphLoader] - analyzes JSON style execution plan information
  * [StatementAnalyzer] - analyzes statement execution plan
  * [PropertyExtractor] - extracts information from relational or exchange operators
* [DotGenerator] - emits Graphviz DOT script from execution plan graph

[JsonPlanGraphLoader]:src/main/java/com/tsurugidb/tsubakuro/explain/json/JsonPlanGraphLoader.java
[StatementAnalyzer]:src/main/java/com/tsurugidb/tsubakuro/explain/json/StatementAnalyzer.java
[PropertyExtractor]:src/main/java/com/tsurugidb/tsubakuro/explain/json/PropertyExtractor.java
[DotGenerator]:src/main/java/com/tsurugidb/tsubakuro/explain/DotGenerator.java

## Trying to show diagram

```sh
./gradlew :tsubakuro-explain:run --args /path/to/explain.json
```

## Examples

### Loads execution plan

```java
// explain result text
String jsonText = ...;

// create loader
var loader = JsonPlanGraphLoader.newBuilder()
        .build();

// analyzes execution plan
PlanGraph graph = loader.load(jsonText);
```

### Dumps execution plan

```java
// execution plan graph
PlanGraph graph = ...;

// create generator
var generator = DotGenerator.newBuilder()
        .build();

// write DOT file
try (var output = Files.newBufferedWriter(Path.of(...))) {
    generator.write(graph, output);
}
```

### Configures execution plan graph

```java
var loader = JsonPlanGraphLoader.newBuilder()
        // also show "filter" operators
        .withIncludeOperators(Set.of("filter"))
        .build();
```

```java
var loader = JsonPlanGraphLoader.newBuilder()
        // show all omitted operators ..
        .withNodeFilter(n -> true)
        // .. except edge of processes
        .withExcludeOperators(Set.of("take_flat", "take_group", "take_cogroup", "offer"))
        .build();
```

### Configures diagram style

```java
var generator = DotGenerator.newBuilder()
        // replaces header line
        .withHeader(List.of(
                // "digraph" block
                "digraph {",
                // right to left dataflow
                "rankdir=RL;",
                // all vertices are rectangle
                "node [ shape=rectangle; ]"))
        // also show kind name of operators
        .withShowNodeKind(true)
        .build();
```

## Available statements

* `execute_statement`
  * almost DML statements (e.g. `SELECT`, `UPDATE`, or `DELETE`)
* `write_statement`
  * several `INSERT` statements which does not retrieves rows from any tables (e.g. `INSERT INTO ... VALUES ...`)

## Available operators and properties

### input operators

* `find`
  * title - `scan`
  * attributes
    * `source` - access pattern
      * `table` - find from primary index
      * `index` - find from secondary index
    * `table` - source table name
    * `index` - source secondary index name (optional)
    * `access` - always `point`
* `scan`
  * title - `scan`
  * attributes
    * `source` - access pattern
      * `table` - scan on primary index
      * `index` - scan on secondary index, may or may not access to primary index
    * `table` - source table name
    * `index` - source secondary index name (optional)
    * `access`- access pattern
      * `full-scan` - full scan
      * `range-scan` - range scan
* `values`
  * title - `values`
  * attributes
    * (no attributes)

### output operators

* `emit`
  * title - `emit`
  * attributes
    * (no attributes)
* `write`
  * title - `write`
  * attributes
    * `write-kind` - write operation kind
      * `insert` - insert operation
      * `update` - update operation
      * `delete` - delete operation
      * ...
    * `table` - destination table name

### join operators

* `join_find`
  * title - `join`
  * attributes
    * `join-type` - join direction
      * `inner` - inner join
      * `left_outer` - left outer join
      * `semi` - left semi join
      * `anti` - left anti join
    * `source` - access pattern (right hand side)
      * `table` - find from primary index
      * `index` - find from secondary index
      * `broadcast` - scan on broadcast exchange
    * `table` - source table name (optional)
    * `index` - source secondary index name (optional)
    * `access` - always `point`
* `join_scan`
  * title - `join`
  * attributes
    * `join-type` - join direction
      * `inner` - inner join
      * `left_outer` - left outer join
      * `semi` - left semi join
      * `anti` - left anti join
    * `source` - access pattern (right hand side)
      * `table` - find from primary index
      * `index` - find from secondary index
      * `broadcast` - scan on broadcast exchange
    * `table` - source table name (optional)
    * `index` - source secondary index name (optional)
    * `access`- access pattern
      * `full-scan` - full scan
      * `range-scan` - range scan
* `join_group`
  * title - `join`
  * attributes
    * `join-type` - join direction
      * `inner` - inner join
      * `left_outer` - left outer join
      * `full_outer` - full outer join
      * `semi` - left semi join
      * `anti` - left anti join
    * `source` - always `flow`
    * `access`- always `merge`

### exchange operators

* `forward_exchange`
  * title - `forward`
  * attributes
    * `limit` - the max number of rows to feed to downstream (optional)
* `group_exchange`
  * title - `group`
  * attributes
    * `whole` - whether to construct a single group from the whole relation
    * `sorted` - whether to sort rows in each group
    * `limit` - the max number of rows in each group to feed to downstream (optional)
* `aggregate_exchange`
  * title - `aggregate`
  * attributes
    * `whole` - whether to aggregate the whole relation, or aggregate rows in each group
    * `incremental` - always `true`
* `broadcast_exchange`
  * title - `broadcast`
  * attributes
    * (not available)

### other operators

* `aggregate_group`
  * title - `aggregate`
  * attributes
    * `incremental` - always `false`
* `difference_group`
  * title - `difference`
  * attributes
    * (not available)
* `intersection_group`
  * title - `intersection`
  * attributes
    * (not available)
