package com.sample.cloudtable.runner;

import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * CloudTable测试插入查询数据
 *
 * @author Aaric, created on 2020-05-18T17:07.
 * @version 0.2.0-SNAPSHOT
 */
@Log4j2
@Order(2)
@Component
public class CloudTableRunner implements CommandLineRunner {

    @Autowired
    private Connection hbaseConnection;

    @Override
    public void run(String... args) throws Exception {
        // 测试表
        String tableTest = "test";

        // 创建表
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableTest));
        HColumnDescriptor family = new HColumnDescriptor("base");
        family.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        family.setCompressionType(Compression.Algorithm.SNAPPY);
        desc.addFamily(family);

        Admin admin = hbaseConnection.getAdmin();
        admin.createTable(desc);
        admin.close();

        log.debug("{} created.", tableTest);

        // 操作表
        String rowKey = "rk001";
        Table table = hbaseConnection.getTable(TableName.valueOf(tableTest));

        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("col01"), Bytes.toBytes("hello world"));
        table.put(put);
        log.debug("put data.");

        // 查询表
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes("base"), Bytes.toBytes("col01"));
        log.debug("get data: {}.", Bytes.toString(value));
    }
}
