package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;

public class Insert {
    Session session;
    
    public Insert(Connector connector, Session session) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
    }
    
    public void insert(String sql) throws IOException, ExecutionException, InterruptedException {
	Transaction transaction = session.createTransaction().get();
	transaction.executeStatement(sql).get();
	transaction.commit().get();
    }

    public void insert() throws IOException, ExecutionException, InterruptedException {
	insert("INSERT INTO ORDERS (o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (99999999, 1234, 3, 1, '20210620', 3, 7, 0)");
    }
}
