package com.nautilus_technologies.tsubakuro.examples.tpcc;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

public class NewOrder {
    SqlClient sqlClient;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared1;
    PreparedStatement prepared2;
    PreparedStatement prepared3;
    PreparedStatement prepared4;
    PreparedStatement prepared5;
    PreparedStatement prepared6;
    PreparedStatement prepared7;
    PreparedStatement prepared8;
    PreparedStatement prepared9;

    long warehouses;
    long paramsWid;
    long paramsDid;
    long paramsCid;
    long paramsOlCnt;
    boolean paramsRemoteWarehouse;
    long paramsSupplyWid;
    long[] paramsQty;
    long[] paramsItemId;
    boolean paramsWillRollback;
    String paramsEntryD;
    long paramsAllLocal;

    long[] stock;
    String[] bg;
    double[] amt;
    double total;
    double[] price;
    String[] iNames;

    //  local variables
    double wTax;
    double cDiscount;
    String cLast;
    String cCredit;
    long dNextOid;
    double dTax;
    long oid;
    double iPrice;
    String iName;
    String iData;
    long sQuantity;
    String sData;
    String[] sDistData;

    public NewOrder(SqlClient sqlClient, RandomGenerator randomGenerator, Profile profile) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
        this.randomGenerator = randomGenerator;
        this.warehouses = profile.warehouses;
        this.profile = profile;
        this.paramsQty = new long[15];
        this.paramsItemId = new long[15];

        this.stock = new long[15];
        this.bg = new String[15];
        this.amt = new double[15];
        this.price = new double[15];
        this.iNames = new String[15];
        this.sDistData = new String[10];
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
        String sql1 = "SELECT w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :c_d_id AND c_id = :c_id";
        prepared1 = sqlClient.prepare(sql1,
            Placeholders.of("w_id", long.class),
            Placeholders.of("c_d_id", long.class),
            Placeholders.of("c_id", long.class)).get();
    
        String sql2 = "SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
        prepared2 = sqlClient.prepare(sql2,
            Placeholders.of("d_w_id", long.class),
            Placeholders.of("d_id", long.class)).get();
    
        String sql3 = "UPDATE DISTRICT SET d_next_o_id = :d_next_o_id WHERE d_w_id = :d_w_id AND d_id = :d_id";
        prepared3 = sqlClient.prepare(sql3,
            Placeholders.of("d_next_o_id", long.class),
            Placeholders.of("d_w_id", long.class),
            Placeholders.of("d_id", long.class)).get();
    
        String sql4 = "INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (:o_id, :o_d_id, :o_w_id, :o_c_id, :o_entry_d, :o_ol_cnt, :o_all_local)";
        prepared4 = sqlClient.prepare(sql4,
            Placeholders.of("o_id", long.class),
            Placeholders.of("o_d_id", long.class),
            Placeholders.of("o_w_id", long.class),
            Placeholders.of("c_d_id", long.class),
            Placeholders.of("o_c_id", long.class),
            Placeholders.of("o_entry_d", String.class),
            Placeholders.of("o_ol_cnt", long.class),
            Placeholders.of("o_all_local", long.class)).get();
    
        String sql5 = "INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id)";
        prepared5 = sqlClient.prepare(sql5,
            Placeholders.of("no_o_id", long.class),
            Placeholders.of("no_d_id", long.class),
            Placeholders.of("no_w_id", long.class)).get();
    
        String sql6 = "SELECT i_price, i_name , i_data FROM ITEM WHERE i_id = :i_id";
        prepared6 = sqlClient.prepare(sql6,
            Placeholders.of("i_id", long.class)).get();
    
        String sql7 = "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM STOCK WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id";
        prepared7 = sqlClient.prepare(sql7,
            Placeholders.of("s_i_id", long.class),
            Placeholders.of("s_w_id", long.class)).get();
    
        String sql8 = "UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id";
        prepared8 = sqlClient.prepare(sql8,
            Placeholders.of("s_quantity", long.class),
            Placeholders.of("s_i_id", long.class),
            Placeholders.of("s_w_id", long.class)).get();
    
        String sql9 = "INSERT INTO ORDER_LINE (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)VALUES (:ol_o_id, :ol_d_id, :ol_w_id, :ol_number, :ol_i_id, :ol_supply_w_id, :ol_quantity, :ol_amount, :ol_dist_info)";
        prepared9 = sqlClient.prepare(sql9,
            Placeholders.of("ol_o_id", long.class),
            Placeholders.of("ol_d_id", long.class),
            Placeholders.of("ol_w_id", long.class),
            Placeholders.of("ol_number", long.class),
            Placeholders.of("ol_i_id", long.class),
            Placeholders.of("ol_supply_w_id", long.class),
            Placeholders.of("ol_quantity", long.class),
            Placeholders.of("ol_amount", double.class),
            Placeholders.of("ol_dist_info", String.class)).get();
    }

    static String timeStamp() {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd (EEE) HH:mm:ss", new Locale("US"));
        return dateFormat.format(date);
    }

    public void setParams() {
        if (profile.fixThreadMapping) {
            long warehouseStep = warehouses / profile.threads;
            paramsWid = randomGenerator.uniformWithin((profile.index * warehouseStep) + 1, (profile.index + 1) * warehouseStep);
        } else {
            paramsWid = randomGenerator.uniformWithin(1, warehouses);
        }
        paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
        paramsCid = randomGenerator.uniformWithin(1, Scale.CUSTOMERS);  // scale::customers
        paramsOlCnt = randomGenerator.uniformWithin(Scale.MIN_OL_COUNT, Scale.MAX_OL_COUNT); // scale::min_ol_count, scale::max_ol_count

        paramsRemoteWarehouse = (randomGenerator.uniformWithin(1, 100) <= Percent.K_NEW_ORDER_REMOTE); //kNewOrderRemotePercent
        if (paramsRemoteWarehouse && warehouses > 1) {
            do {
                    paramsSupplyWid = randomGenerator.uniformWithin(1, warehouses);
            } while (paramsSupplyWid != paramsWid);
            paramsAllLocal = 0;
        } else {
            paramsSupplyWid = paramsWid;
            paramsAllLocal = 1;
        }

        for (int ol = 1; ol <= paramsOlCnt; ++ol) {
            paramsQty[ol - 1] = randomGenerator.uniformWithin(1, 10);
            paramsItemId[ol - 1] = randomGenerator.nonUniform8191Within(1, Scale.ITEMS); // scale::items
        }
        paramsEntryD = timeStamp();
        paramsWillRollback = (randomGenerator.uniformWithin(1, 100) == 1);
    }

    void rollback() throws IOException, ServerException, InterruptedException {
        try {
            transaction.rollback().get();
        } finally {
            transaction = null;
        }
    }

    @SuppressWarnings("checkstyle:methodlength")
    public void transaction(AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
        while (!stop.get()) {
            transaction = sqlClient.createTransaction().get();
            profile.invocation.newOrder++;
            total = 0;
    
            // SELECT w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :c_d_id AND c_id = :c_id;
            var future1 = transaction.executeQuery(prepared1,
            Parameters.of("w_id", (long) paramsWid),
            Parameters.of("c_d_id", (long) paramsDid),
            Parameters.of("c_id", (long) paramsCid));
            try (var resultSet1 = future1.get()) {
                if (!resultSet1.nextRow()) {
                    throw new IOException("no record");
                }
                resultSet1.nextColumn();
                wTax = resultSet1.fetchFloat8Value();
                resultSet1.nextColumn();
                cDiscount = resultSet1.fetchFloat8Value();
                resultSet1.nextColumn();
                cLast = resultSet1.fetchCharacterValue();
                resultSet1.nextColumn();
                cCredit = resultSet1.fetchCharacterValue();
                if (resultSet1.nextRow()) {
                    throw new IOException("found multiple records");
                }
            } catch (ServerException e) {
                profile.retryOnStatement.newOrder++;
                profile.customerTable.newOrder++;
                rollback();
                continue;
            }
    
            // SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id
            var future2 = transaction.executeQuery(prepared2,
            Parameters.of("d_w_id", (long) paramsWid),
            Parameters.of("d_id", (long) paramsDid));
            try (var resultSet2 = future2.get()) {
                if (!resultSet2.nextRow()) {
                    throw new IOException("no record");
                }
                resultSet2.nextColumn();
                dNextOid = resultSet2.fetchInt8Value();
                resultSet2.nextColumn();
                dTax = resultSet2.fetchFloat8Value();
                if (resultSet2.nextRow()) {
                    throw new IOException("found multiple records");
                }
            } catch (ServerException e) {
                profile.retryOnStatement.newOrder++;
                profile.districtTable.newOrder++;
                rollback();
                continue;
            }
    
            try {
                // UPDATE DISTRICT SET d_next_o_id = :d_next_o_id WHERE d_w_id = :d_w_id AND d_id = :d_id
                var future3 = transaction.executeStatement(prepared3,
                    Parameters.of("d_next_o_id", (long) (dNextOid + 1)),
                    Parameters.of("d_w_id", (long) paramsWid),
                    Parameters.of("d_id", (long) paramsDid));
                var result3 = future3.get();
            } catch (ServerException e) {
                profile.retryOnStatement.newOrder++;
                profile.districtTable.newOrder++;
                rollback();
                continue;
            }
    
            oid = dNextOid;
    
            try {
                // INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (:o_id, :o_d_id, :o_w_id, :o_c_id, :o_entry_d, :o_ol_cnt, :o_all_local
                var future4 = transaction.executeStatement(prepared4,
                    Parameters.of("o_id", (long) oid),
                    Parameters.of("o_d_id", (long) paramsDid),
                    Parameters.of("o_w_id", (long) paramsWid),
                    Parameters.of("o_c_id", (long) paramsCid),
                    Parameters.of("o_entry_d", paramsEntryD),
                    Parameters.of("o_ol_cnt", (long) paramsOlCnt),
                    Parameters.of("o_all_local", (long) paramsAllLocal));
                var result4 = future4.get();
            } catch (ServerException e) {
                profile.retryOnStatement.newOrder++;
                profile.ordersTable.newOrder++;
                rollback();
                continue;
            }
    
            try {
                // INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id
                var future5 = transaction.executeStatement(prepared5,
                    Parameters.of("no_o_id", (long) oid),
                    Parameters.of("no_d_id", (long) paramsDid),
                    Parameters.of("no_w_id", (long) paramsWid));
                var result5 = future5.get();
            } catch (ServerException e) {
                profile.retryOnStatement.newOrder++;
                profile.ordersTable.newOrder++;
                rollback();
                continue;
            }
    
            int olNumber;
            for (olNumber = 1; olNumber <= paramsOlCnt; olNumber++) {
                var olSupplyWid = paramsSupplyWid;
                var olIid = paramsItemId[olNumber - 1];
                var olQuantity = paramsQty[olNumber - 1];
        
                // SELECT i_price, i_name , i_data FROM ITEM WHERE i_id = :i_id
                var future6 = transaction.executeQuery(prepared6,
                Parameters.of("i_id", (long) olIid));
                try (var resultSet6 = future6.get()) {
                    if (!resultSet6.nextRow()) {
                        throw new IOException("no record");
                    }
                    resultSet6.nextColumn();
                    iPrice = resultSet6.fetchFloat8Value();
                    resultSet6.nextColumn();
                    iName = resultSet6.fetchCharacterValue();
                    resultSet6.nextColumn();
                    iData = resultSet6.fetchCharacterValue();
                    if (resultSet6.nextRow()) {
                        throw new IOException("found multiple records");
                    }
                } catch (ServerException e) {
                    break;
                }
    
                // SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM STOCK WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id
                var future7 = transaction.executeQuery(prepared7,
                Parameters.of("s_i_id", (long) olIid),
                Parameters.of("s_w_id", (long) olSupplyWid));
                try (var resultSet7 = future7.get()) {
                    if (!resultSet7.nextRow()) {
                        throw new IOException("no record");
                    }
                    resultSet7.nextColumn();
                    sQuantity = resultSet7.fetchInt8Value();
                    resultSet7.nextColumn();
                    sData = resultSet7.fetchCharacterValue();
                    for (int i = 0; i < 10; i++) {
                        resultSet7.nextColumn();
                        sDistData[i] = resultSet7.fetchCharacterValue();
                    }
                    if (resultSet7.nextRow()) {
                        throw new IOException("found multiple records");
                    }
                } catch (ServerException e) {
                    profile.stockTable.newOrder++;
                    break;
                }
    
                String olDistInfo = sDistData[(int) paramsDid - 1].substring(0, 24);
                stock[olNumber - 1] = sQuantity;
        
                if (iData.indexOf("original") >= 0 && sData.indexOf("original") >= 0) {
                    bg[olNumber - 1] = "B";
                } else {
                    bg[olNumber - 1] = "G";
                }
        
                if (sQuantity > olQuantity) {
                    sQuantity = sQuantity - olQuantity;
                } else {
                    sQuantity = sQuantity - olQuantity + 91;
                }
        
                try {
                    // UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id
                    var future8 = transaction.executeStatement(prepared8,
                        Parameters.of("s_quantity", (long) sQuantity),
                        Parameters.of("s_i_id", (long) olIid),
                        Parameters.of("s_w_id", (long) olSupplyWid));
                    var result8 = future8.get();
                } catch (ServerException e) {
                    profile.stockTable.newOrder++;
                    break;
                }
        
                double olAmount = olQuantity * iPrice * (1 + wTax + dTax) * (1 - cDiscount);
                amt[olNumber - 1] = olAmount;
                total += olAmount;
    
                try {
                    // INSERT INTO ORDER_LINE (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)VALUES (:ol_o_id, :ol_d_id, :ol_w_id, :ol_number, :ol_i_id, :ol_supply_w_id, :ol_quantity, :ol_amount, :ol_dist_info
                    var future9 = transaction.executeStatement(prepared9,
                        Parameters.of("ol_o_id", (long) oid),
                        Parameters.of("ol_d_id", (long) paramsDid),
                        Parameters.of("ol_w_id", (long) paramsWid),
                        Parameters.of("ol_number", (long) olNumber),
                        Parameters.of("ol_i_id", (long) olIid),
                        Parameters.of("ol_supply_w_id", (long) olSupplyWid),
                        Parameters.of("ol_quantity", (long) olQuantity),
                        Parameters.of("ol_amount", (long) olAmount),
                        Parameters.of("ol_dist_info", olDistInfo));
                    var result9 = future9.get();
                } catch (ServerException e) {
                    profile.ordersTable.newOrder++;
                    break;
                }
            }

            if (olNumber > paramsOlCnt) {  // completed 'for (olNumber = 1; olNumber <= paramsOlCnt; olNumber++) {'
                if (paramsWillRollback) {
                    rollback();
                    profile.newOrderIntentionalRollback++;
                    return;
                }
                try {
                    transaction.commit().get();
                    profile.completion.newOrder++;
                    return;
                } catch (ServerException e) {
                    profile.retryOnCommit.newOrder++;
                    transaction = null;
                    continue;
                }
            }

            // break in 'for (olNumber = 1; olNumber <= paramsOlCnt; olNumber++) {'
            profile.retryOnStatement.newOrder++;
            rollback();
        }
    }
}
