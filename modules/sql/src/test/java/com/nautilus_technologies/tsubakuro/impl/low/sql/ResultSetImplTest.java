package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ProtosForTest;

import org.junit.jupiter.api.Test;

class ResultSetImplTest {
    private ResultSetImpl resultSetImpl;
    private ResultSet.RecordMeta recordMeta;

    @Test
    void receiveSchemaMetaAndRecord() {
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock());

	    recordMeta = resultSetImpl.getRecordMeta();
	    assertTrue(recordMeta.fieldCount() == 6);
	    assertTrue(recordMeta.at(0) == CommonProtos.DataType.INT8);
	    assertTrue(recordMeta.nullable(0) == false);
	    assertTrue(recordMeta.at(1) == CommonProtos.DataType.FLOAT8);
	    assertTrue(recordMeta.nullable(1) == false);
	    assertTrue(recordMeta.at(2) == CommonProtos.DataType.STRING);
	    assertTrue(recordMeta.nullable(2) == true);
	    assertTrue(recordMeta.at(3) == CommonProtos.DataType.INT8);
	    assertTrue(recordMeta.nullable(3) == false);
	    assertTrue(recordMeta.at(4) == CommonProtos.DataType.FLOAT8);
	    assertTrue(recordMeta.nullable(4) == false);
	    assertTrue(recordMeta.at(5) == CommonProtos.DataType.STRING);
	    assertTrue(recordMeta.nullable(5) == true);

	    assertTrue(resultSetImpl.nextRecord());
	    assertTrue(resultSetImpl.nextColumn());
	    assertTrue(resultSetImpl.getInt8() == 987654321L);
	    assertTrue(resultSetImpl.nextColumn());
	    assertTrue(resultSetImpl.getFloat8() == (double) 12345.6789);
	    assertTrue(resultSetImpl.nextColumn());
	    assertTrue(resultSetImpl.getCharacter().equals("This is a string for the test"));
	    assertTrue(resultSetImpl.nextColumn());
	    assertTrue(resultSetImpl.getInt8() == (long) 123456789L);
	    assertTrue(resultSetImpl.nextColumn());
	    assertTrue(resultSetImpl.getFloat8() == (double) 98765.4321);
	    assertTrue(resultSetImpl.nextColumn());
	    assertTrue(resultSetImpl.isNull());
	    assertFalse(resultSetImpl.nextColumn());
	    assertFalse(resultSetImpl.nextRecord());
	    
	} catch (IOException e) {
            fail("cought IOException");
        }
    }
}
