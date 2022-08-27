package com.tsurugidb.tsubakuro.console.parser;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.tsurugidb.tsubakuro.console.model.Regioned;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ExclusiveMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ReadWriteMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.TransactionMode;
import com.tsurugidb.tsubakuro.console.model.Value;

class StartTransactionCandidate {

    Regioned<TransactionMode> transactionMode;

    Regioned<ReadWriteMode> readWriteMode;

    Regioned<ExclusiveMode> exclusiveMode;

    List<Regioned<String>> writePreserve;

    List<Regioned<String>> readAreaInclude;

    List<Regioned<String>> readAreaExclude;

    Regioned<String> label;

    Map<Regioned<String>, Optional<Regioned<Value>>> properties;
}
