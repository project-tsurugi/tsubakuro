package com.tsurugidb.tsubakuro.console.parser;

import java.util.List;

import com.tsurugidb.tsubakuro.console.model.Regioned;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ExclusiveMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ReadWriteMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.TransactionMode;

class StartTransactionCandidate {

    Regioned<TransactionMode> transactionMode;

    Regioned<ReadWriteMode> readWriteMode;

    Regioned<ExclusiveMode> exclusiveMode;

    List<Regioned<String>> writePreserve;

    List<Regioned<String>> readAreaInclude;

    List<Regioned<String>> readAreaExclude;

    Regioned<String> label;
}
