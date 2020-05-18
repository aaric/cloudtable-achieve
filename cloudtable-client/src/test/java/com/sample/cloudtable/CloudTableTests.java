package com.sample.cloudtable;

import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * CloudTableTests
 *
 * @author Aaric, created on 2020-05-18T11:12.
 * @version 0.0.1-SNAPSHOT
 */
@Log4j2
@SpringBootTest
@ExtendWith(SpringExtension.class)
public class CloudTableTests {

    /**
     * 测试表
     */
    private String tableTest = "test";

    @Autowired
    private Connection hbaseConnection;

    @Test
    @Disabled
    public void testCreateTable() throws Exception {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableTest));
        HColumnDescriptor family = new HColumnDescriptor("base");
        family.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        family.setCompressionType(Compression.Algorithm.SNAPPY);
        table.addFamily(family);

        Admin admin = hbaseConnection.getAdmin();
        admin.createTable(table);
        admin.close();
    }

    @Test
    public void testPut() throws Exception {
        Table table = hbaseConnection.getTable(TableName.valueOf(tableTest));

        Put put = new Put(Bytes.toBytes("rk001"));
        put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("col01"), Bytes.toBytes("hello world"));

        table.put(put);
    }

    @Test
    @Disabled
    public void testGet() throws Exception {
        Table table = hbaseConnection.getTable(TableName.valueOf(tableTest));

        Get get = new Get(Bytes.toBytes("rk001"));
        Result result = table.get(get);

        byte[] value = result.getValue(Bytes.toBytes("base"), Bytes.toBytes("col01"));
        log.debug(Bytes.toString(value));

    }
}
