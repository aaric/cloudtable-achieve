package com.huawei.cloudtable.lemonIndex.examples;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.lemon.client.*;
import org.lemon.common.LemonConstants;
import org.lemon.index.BitmapIndexDescriptor;
import org.lemon.index.FamilyOnlyName;
import org.lemon.index.IndexUtils;
import org.lemon.index.TermExtractor;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class LemonIndexSample {
  private final static Log LOG = LogFactory.getLog(LemonIndexSample.class.getName());
  private Connection conn;
  private static final TableName tableName = TableName.valueOf("TableOfLemonIndex");

  private static final String SPLIT = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G," +
    "H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,1,2,3,4,5,6,7";
  private static final int SHARD_NUM = 60;
  private static final String FAM_M_STR = "M";
  private static final String FAM_N_STR = "N";
  private static final String QUA_M_STR = "qua_m";
  private static final String QUA_N_STR = "qua_n";
  private static final byte[] FAM_M = Bytes.toBytes(FAM_M_STR);
  private static final byte[] FAM_N = Bytes.toBytes(FAM_N_STR);
  private static final byte[] QUA_M = Bytes.toBytes(QUA_M_STR);
  private static final byte[] QUA_N = Bytes.toBytes(QUA_N_STR);


  public LemonIndexSample(Configuration conf) throws IOException {
    this.conn = ConnectionFactory.createConnection(conf);
  }

  public void test() throws Exception {
    try {
      testCreateTable();
      testPut();
      testQuery();
      dropTable();
    } catch (Exception e) {
      throw e;
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (Exception e1) {
          LOG.error("Failed to close the connection ", e1);
        }
      }
    }
  }

  /**
   * Create user info table
   */
  public void testCreateTable() {
    LOG.info("Entering testCreateTable.");
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor cdm = new HColumnDescriptor(FAM_M);
    cdm.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    tableDesc.addFamily(cdm);
    HColumnDescriptor cdn = new HColumnDescriptor(FAM_N);
    cdn.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    tableDesc.addFamily(cdn);

    // Add bitmap index definitions.
    List<BitmapIndexDescriptor> bitmaps = new ArrayList<>();
    bitmaps.add(BitmapIndexDescriptor.builder()
      // Describe which column should be indexed.
      .setColumnName(FamilyOnlyName.valueOf(FAM_M))
      // Describe how to extract term(s) from KeyValue
      .setTermExtractor(TermExtractor.NAME_VALUE_EXTRACTOR)
      .build());
    // It will help to add several properties into HTableDescriptor.
    IndexHelper.enableAutoIndex(tableDesc, SHARD_NUM, bitmaps);

    List<byte[]> splitList = Arrays.stream(SPLIT.split(LemonConstants.COMMA))
      .map(s -> org.lemon.common.Bytes.toBytes(s.trim()))
      .collect(Collectors.toList());
    byte[][] splitArray = splitList.toArray(new byte[splitList.size()][]);

    Admin admin = null;
    try {
      // Instantiate an Admin object.
      admin = conn.getAdmin();
      if (!admin.tableExists(tableName)) {
        LOG.info("Creating table...");
        admin.createTable(tableDesc, splitArray);
        LOG.info(admin.getClusterStatus());
        LOG.info(admin.listNamespaceDescriptors());
        LOG.info("Table created successfully.");
      } else {
        LOG.warn("table already exists");
      }
    } catch (IOException e) {
      LOG.error("Create table failed.", e);
    } finally {
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting testCreateTable.");
  }

  /**
   * same with HBase put API
   */
  public void testPut() {
    LOG.info("Entering testPut.");

    // Specify the column family name.
    byte[] familyName = Bytes.toBytes("info");
    // Specify the column name.
    byte[][] qualifiers = { Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"),
        Bytes.toBytes("address") };

    Table table = null;
    try {
      // Instantiate an HTable object.
      table = conn.getTable(tableName);
      List<Put> puts = new ArrayList<>();

      // Instantiate a Put object.
      Put put = new Put(Bytes.toBytes("rowkey001"));
      put.addColumn(FAM_M, QUA_M, Bytes.toBytes("A"));
      put.addColumn(FAM_N, QUA_N, Bytes.toBytes("B"));
      puts.add(put);

      put = new Put(Bytes.toBytes("rowkey002"));
      put.addColumn(FAM_M, QUA_M, Bytes.toBytes("C"));
      put.addColumn(FAM_N, QUA_N, Bytes.toBytes("D"));
      puts.add(put);

      put = new Put(Bytes.toBytes("rowkey003"));
      put.addColumn(FAM_M, QUA_M, Bytes.toBytes("E"));
      put.addColumn(FAM_N, QUA_N, Bytes.toBytes("F"));
      puts.add(put);

      // Submit a put request.
      table.put(puts);

      LOG.info("Put successfully.");
    } catch (IOException e) {
      LOG.error("Put failed ", e);
    } finally {
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          LOG.error("Close table failed ", e);
        }
      }
    }
    LOG.info("Exiting testPut.");
  }


  /**
   * Query Data
   */
  public void testQuery() {
    LOG.info("Entering testQuery.");

    try(Table table = conn.getTable(tableName)) {
      // Using Table instance to create LemonTable.
      LemonTable lemonTable = new LemonTable(table);
      // Build LemonQuery.
      LemonQuery query = LemonQuery.builder()
      // Set ad-hoc query condition.
      .setQuery("qua_m:A OR qua_m:C")
      // Set how many rows should be cached on client for the initial request.
      .setCaching(10)
        // Set return column family/columns.
        //.addFamily(FAM_M)
        .build();
      ResultSet resultSet = lemonTable.query(query);
      // Read result rows.

      List<EntityEntry> entries = resultSet.listRows();
      for (EntityEntry entry : entries) {
        Map<String, Map<String, String>> fams = entry.getColumns();
        for (Map.Entry<String, Map<String, String>> familyEntry : fams.entrySet()) {
          String family = familyEntry.getKey();
          Map<String, String> qualifiers = familyEntry.getValue();
          for (Map.Entry<String, String> qualifer : qualifiers.entrySet()) {
            String Qua = qualifer.getKey();
            String value = qualifer.getValue();
            LOG.info("rowkey is " + Bytes.toString(entry.getRow()) + ", qualifier is "
              + family + ":" + Qua + ", value is " + value);
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Get data failed ", e);
    }

    LOG.info("Exiting testGet.");
  }

  /**
   * Delete user table
   */
  public void dropTable() {
    LOG.info("Entering dropTable.");

    Admin admin = null;
    try {
      admin = conn.getAdmin();
      if (admin.tableExists(tableName)) {
        TableName indexTable = IndexUtils.getInvertedIndexTableName(tableName);
        if (admin.tableExists(indexTable)) {
          admin.disableTable(indexTable);
          admin.deleteTable(indexTable);
        }

        if (admin.tableExists(tableName)) {
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
        }
      }
      LOG.info("Drop table successfully.");
    } catch (IOException e) {
      LOG.error("Drop table failed ", e);
    } finally {
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Close admin failed ", e);
        }
      }
    }
    LOG.info("Exiting dropTable.");
  }
}
