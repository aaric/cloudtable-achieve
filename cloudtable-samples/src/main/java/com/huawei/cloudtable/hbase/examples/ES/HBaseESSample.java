package com.huawei.cloudtable.hbase.examples.ES;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ESColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.lemon.es.ESDocIndexException;
import org.lemon.es.HBaseESConst;

import java.io.IOException;
import java.util.*;

/**
 * HBase Development Instruction Sample Code The sample code uses user
 * information as source data,it introduces how to implement businesss process
 * development using HBase API
 */
public class HBaseESSample {
  private final static Log LOG = LogFactory.getLog(HBaseESSample.class.getName());

  private final String ESClusterHosts = "192.168.0.185:9200,192.168.0.45:9200";
  private final String table = "HBaseES_sample_table";
  private final String ES_INDEX_NAME = "article";
  private final byte[] CF1 = Bytes.toBytes("cf1");
  private final byte[] CF2 = Bytes.toBytes("cf2");
  private final byte[] QUA_ARTICLE_CONTENT_CHINESE = Bytes.toBytes("contentCh");//text
  private final byte[] QUA_ARTICLE_CONTENT_English = Bytes.toBytes("contentEng");//text
  private final byte[] QUA_ARTICLE_ID = Bytes.toBytes("id");//long
  private final byte[] QUA_CHARACTER_NUM = Bytes.toBytes("charNum");//integer
  private final byte[] QUA_PAGE_NUM = Bytes.toBytes("pageNum");//short
  // A means perfect, B means good, C means normal
  private final byte[] QUA_ARTICLE_LEVEL = Bytes.toBytes("level");//byte
  private final byte[] QUA_RESEARCH_COST = Bytes.toBytes("researchCost");//double
  private final byte[] QUA_ARTICLE_SCORE = Bytes.toBytes("score");//float
  private final byte[] QUA_AUTHOR_MALE = Bytes.toBytes("male");//boolean
  private final byte[] QUA_WHATEVER = Bytes.toBytes("whatever");

  private TableName tableName = null;
  private Connection conn = null;

  public HBaseESSample(Configuration conf) throws IOException {
    this.tableName = TableName.valueOf(table);
    this.conn = ConnectionFactory.createConnection(conf);

    assert this.conn instanceof LemonConnectionImplementation;
  }

  public void test() throws Exception {
    try {
      createTable();
      putData();
      // invoke the wrapper scan api
      // we can not search the docs immediately after the docs indexed, so sleep a while
      // Elasticsearch by default refreshes each shard every 1s, so the document will be
      // available to search 1s after indexing it.
      Thread.sleep(1000);
      testScanDataWithES1();
      testScanDataWithES2();
      testScanDataByPrimaryDataType();
      testScanDataByEnglishKeyword();
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
   *
   * create table opening ES indexing should use the following schema:
   * METADATA => {
   * 'hbase.index.es.enabled' => 'true',
   * 'hbase.index.es.endpoint'=>'8.5.131.1:9200,8.5.131.2',
   * 'hbase.index.es.indexname'=>'article',
   * 'hbase.index.es.schema' => '[
   * {"name":"contentCh","type":"text","hbaseQualifier":"cf1:contentCh","analyzer":"ik_smart"},
   * {"name":"contentEng","type":"text","hbaseQualifier":"cf2:contentEng"},
   * {"name":"id","type":"long","hbaseQualifier":"cf1:id"},
   * {"name":"charNum","type":"integer","hbaseQualifier":"cf1:charNum"},
   * {"name":"pageNum","type":"short","hbaseQualifier":"cf1:pageNum"},
   * {"name":"level","type":"byte","hbaseQualifier":"cf1:level"},
   * {"name":"researchCost","type":"double","hbaseQualifier":"cf1:researchCost"},
   * {"name":"score","type":"float","hbaseQualifier":"cf1:score"}
   * {"name":"male","type":"boolean","hbaseQualifier":"cf1:male"}
   * ]'
   * }
   */
  public void createTable() {
    LOG.info("Entering testCreateTable.");

    // Specify the table descriptor.
    HTableDescriptor htd = new HTableDescriptor(tableName);

    // Set the column family name to info.
    HColumnDescriptor hcd = new HColumnDescriptor(CF1);

    // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
    // and PREFIX_TREE
    hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

    // Set compression methods, HBase provides two default compression
    // methods:GZ and SNAPPY
    // GZ has the highest compression rate,but low compression and
    // decompression effeciency,fit for cold data
    // SNAPPY has low compression rate, but high compression and
    // decompression effeciency,fit for hot data.
    // it is advised to use SANPPY
    hcd.setCompressionType(Compression.Algorithm.SNAPPY);

    HColumnDescriptor hcd2 = new HColumnDescriptor(CF2);
    hcd2.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    hcd2.setCompressionType(Compression.Algorithm.SNAPPY);

    htd.addFamily(hcd);
    htd.addFamily(hcd2);

    //add HBase ES schema
    String ESJsonSchema = "[" +
      "{\"name\":\"contentCh\",\"type\":\"text\",\"hbaseQualifier\":\"cf1:contentCh\",\"analyzer\":\"ik_smart\"}," +
      "{\"name\":\"contentEng\",\"type\":\"text\",\"hbaseQualifier\":\"cf2:contentEng\"}," +
      "{\"name\":\"id\",\"type\":\"long\",\"hbaseQualifier\":\"cf1:id\"}," +
      "{\"name\":\"charNum\",\"type\":\"integer\",\"hbaseQualifier\":\"cf1:charNum\"}," +
      "{\"name\":\"pageNum\",\"type\":\"short\",\"hbaseQualifier\":\"cf1:pageNum\"}," +
      "{\"name\":\"level\",\"type\":\"byte\",\"hbaseQualifier\":\"cf1:level\"}," +
      "{\"name\":\"researchCost\",\"type\":\"double\",\"hbaseQualifier\":\"cf1:researchCost\"}," +
      "{\"name\":\"score\",\"type\":\"float\",\"hbaseQualifier\":\"cf1:score\"}," +
      "{\"name\":\"male\",\"type\":\"boolean\",\"hbaseQualifier\":\"cf1:male\"}" +
      "]";
    htd.setValue(HBaseESConst.HBASE_INDEX_ES_ENABLED, "true");
    htd.setValue(HBaseESConst.HBASE_INDEX_ES_ENDPOINT, ESClusterHosts);
    htd.setValue(HBaseESConst.HBASE_INDEX_ES_INDEXNAME, ES_INDEX_NAME);
    htd.setValue(HBaseESConst.HBASE_INDEX_ES_SCHEMA, ESJsonSchema);

    Admin admin = null;
    try {
      // Instantiate an Admin object.
      admin = conn.getAdmin();
      if (!admin.tableExists(tableName)) {
        LOG.info("Creating table...");
        admin.createTable(htd);
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
   * Insert data
   */
  public void putData() {
    LOG.info("Entering testPut.");

    Table table = null;
    try {
      // Instantiate an HTable object.
      table = conn.getTable(tableName);
      List<Put> puts = new ArrayList<Put>();
      // Instantiate a Put object.
      Put put = new Put(Bytes.toBytes("rowkey001"));
      put.addColumn(CF1, QUA_ARTICLE_CONTENT_CHINESE, Bytes.toBytes("夏季我们游泳"));
      put.addColumn(CF2, QUA_ARTICLE_CONTENT_English, Bytes.toBytes("how many apples in the market"));
      put.addColumn(CF1, QUA_ARTICLE_ID, Bytes.toBytes(1111l));
      put.addColumn(CF1, QUA_CHARACTER_NUM, Bytes.toBytes(1));
      short shortNum = 1;
      put.addColumn(CF1, QUA_PAGE_NUM, Bytes.toBytes(shortNum));
      char c = 'A';
      put.addColumn(CF1, QUA_ARTICLE_LEVEL, new byte[]{(byte)c});
      put.addColumn(CF1, QUA_RESEARCH_COST, Bytes.toBytes(111.11d));
      put.addColumn(CF1, QUA_ARTICLE_SCORE, Bytes.toBytes(80.5f));
      put.addColumn(CF1, QUA_AUTHOR_MALE, Bytes.toBytes(true));
      put.addColumn(CF1, QUA_WHATEVER, Bytes.toBytes("happy life and sweet love"));
      puts.add(put);

      put = new Put(Bytes.toBytes("rowkey002"));
      put.addColumn(CF1, QUA_ARTICLE_CONTENT_CHINESE, Bytes.toBytes("冬季我们滑雪和滑冰"));
      put.addColumn(CF2, QUA_ARTICLE_CONTENT_English, Bytes.toBytes("how many people in the swimming pool"));
      put.addColumn(CF1, QUA_ARTICLE_ID, Bytes.toBytes(2222l));
      put.addColumn(CF1, QUA_CHARACTER_NUM, Bytes.toBytes(2));
      shortNum = 2;
      put.addColumn(CF1, QUA_PAGE_NUM, Bytes.toBytes(shortNum));
      c = 'B';
      put.addColumn(CF1, QUA_ARTICLE_LEVEL, new byte[]{(byte)c});
      put.addColumn(CF1, QUA_RESEARCH_COST, Bytes.toBytes(222.22d));
      put.addColumn(CF1, QUA_ARTICLE_SCORE, Bytes.toBytes(170.5f));
      put.addColumn(CF1, QUA_AUTHOR_MALE, Bytes.toBytes(true));
      put.addColumn(CF1, QUA_WHATEVER, Bytes.toBytes("forever young"));
      puts.add(put);

      put = new Put(Bytes.toBytes("rowkey003"));
      put.addColumn(CF1, QUA_ARTICLE_CONTENT_CHINESE, Bytes.toBytes("夏季我们捕捉小蝌蚪"));
      put.addColumn(CF2, QUA_ARTICLE_CONTENT_English, Bytes.toBytes("we play video game in night"));
      put.addColumn(CF1, QUA_ARTICLE_ID, Bytes.toBytes(3333l));
      put.addColumn(CF1, QUA_CHARACTER_NUM, Bytes.toBytes(3));
      shortNum = 3;
      put.addColumn(CF1, QUA_PAGE_NUM, Bytes.toBytes(shortNum));
      c = 'C';
      put.addColumn(CF1, QUA_ARTICLE_LEVEL, new byte[]{(byte)c});
      put.addColumn(CF1, QUA_RESEARCH_COST, Bytes.toBytes(333.33d));
      put.addColumn(CF1, QUA_ARTICLE_SCORE, Bytes.toBytes(180.5f));
      put.addColumn(CF1, QUA_AUTHOR_MALE, Bytes.toBytes(false));
      put.addColumn(CF1, QUA_WHATEVER, Bytes.toBytes("wishes always with you"));
      puts.add(put);

      // Submit a put request.
      try {
        table.put(puts);
      }
      // if your put operation does not contain the all field in your schema, you will get ESDocIndexException.
      // ESDocIndexException means data/document indexing to ES failed, but remember data put in HBase success.
      // so here you handle this exception depends on your business( you can retry or ignore).
      catch (ESDocIndexException e) {
        //TODO
      }

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
   * search condition passed by http parameters
   */
  public void testScanDataWithES1() {
    LOG.info("Entering testScanDataWithES1.");

    Table table = null;
    // Instantiate a ResultScanner object.
    ResultScanner rScanner = null;
    try {
      // Create the Configuration instance.
      table = conn.getTable(tableName);
      assert table instanceof LemonWrapperHTable;

      // set specified qualifier.
      Scan scan = new Scan();
      scan.addColumn(CF1, QUA_ARTICLE_CONTENT_CHINESE);
      scan.addColumn(CF2, QUA_ARTICLE_CONTENT_English);
      scan.addColumn(CF1, QUA_ARTICLE_ID);

      // Set the cache size.
      scan.setCaching(1000);

      // with special filter
      Map<String, String> httpParameters = new HashMap<>();
      httpParameters.put("q", "contentCh:夏季");
      List<String> indexNames = new ArrayList();
      indexNames.add(ES_INDEX_NAME);
      ESColumnValueFilter filter = new ESColumnValueFilter(indexNames, httpParameters, "");
      scan.setFilter(filter);

      // Submit a scan request.
      rScanner = table.getScanner(scan);

      // Print query results.
      for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
        for (Cell cell : r.rawCells()) {
          LOG.info("testScanDataWithES1 by text type condition, scan result:" + Bytes.toString(CellUtil.cloneRow(cell)) + ":"
            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
            + convertCellValue(Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell)));
        }
      }

      LOG.info("Scan data successfully.");
    } catch (IOException e) {
      LOG.error("Scan data failed ", e);
    } finally {
      if (rScanner != null) {
        // Close the scanner object.
        rScanner.close();
      }
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          LOG.error("Close table failed ", e);
        }
      }
    }
    LOG.info("Exiting testScanDataWithES1.");
  }

  /**
   * search condition passed by http JSON body
   */
  public void testScanDataWithES2() {
    LOG.info("Entering testScanDataWithES2.");

    Table table = null;
    // Instantiate a ResultScanner object.
    ResultScanner rScanner = null;
    try {
      // Create the Configuration instance.
      table = conn.getTable(tableName);
      assert table instanceof LemonWrapperHTable;

      // set specified qualifier.
      Scan scan = new Scan();
      scan.addColumn(CF1, QUA_ARTICLE_CONTENT_CHINESE);
      scan.addColumn(CF2, QUA_ARTICLE_CONTENT_English);
      scan.addColumn(CF1, QUA_ARTICLE_ID);

      // Set the cache size.
      scan.setCaching(1000);

      // with special filter
      Map<String, String> httpParameters = Collections.emptyMap();
      List<String> indexNames = new ArrayList();
      indexNames.add(ES_INDEX_NAME);
      String reqBodyJson =
        "{" +
          "    \"query\" : {" +
          "        \"term\" : { \"contentCh\" : \"冬季\" }" +
          "    }" +
          "}";
      ESColumnValueFilter filter = new ESColumnValueFilter(indexNames, httpParameters, reqBodyJson);
      scan.setFilter(filter);

      // Submit a scan request.
      rScanner = table.getScanner(scan);

      // Print query results.
      for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
        for (Cell cell : r.rawCells()) {
          LOG.info("testScanDataWithES2 by text type condition, scan result:" + Bytes.toString(CellUtil.cloneRow(cell)) + ":"
            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
            + convertCellValue(Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell)));
        }
      }

      LOG.info("Scan data successfully.");
    } catch (IOException e) {
      LOG.error("Scan data failed ", e);
    } finally {
      if (rScanner != null) {
        // Close the scanner object.
        rScanner.close();
      }
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          LOG.error("Close table failed ", e);
        }
      }
    }

    LOG.info("Exiting testScanDataWithES2.");
  }

  /**
   * search condition passed by http JSON body, and composed by other primary data type
   */
  public void testScanDataByPrimaryDataType() {
    LOG.info("Entering testScanDataByPrimaryDataType.");

    Table table = null;
    // Instantiate a ResultScanner object.
    ResultScanner rScanner = null;
    try {
      // Create the Configuration instance.
      table = conn.getTable(tableName);
      assert table instanceof LemonWrapperHTable;

      // set specified qualifier.
      Scan scan = new Scan();
      scan.addColumn(CF1, QUA_ARTICLE_CONTENT_CHINESE);
      scan.addColumn(CF2, QUA_ARTICLE_CONTENT_English);
      scan.addColumn(CF1, QUA_ARTICLE_ID);

      // Set the cache size.
      scan.setCaching(1000);

      // with special filter
      Map<String, String> httpParameters = Collections.emptyMap();
      List<String> indexNames = new ArrayList();
      indexNames.add(ES_INDEX_NAME);
      // with special filter
      Map<String, String> httpParametersLong = Collections.emptyMap();
      String reqBodyJson =
        "{" +
          "\"query\" : {" +
          "   \"bool\" : {" +
          "     \"must\" :[" +
          "       {\"term\" : { \"id\" : \"1111\"}}," +
          "       {\"term\" : { \"charNum\" : \"1\"}}," +
          "       {\"term\" : { \"pageNum\" : \"1\"}}," +
          "       {\"term\" : { \"level\" : \"65\"}}," + // 65 means A in ascii
          "       {\"term\" : { \"researchCost\" : \"111.11\"}}," +
          "       {\"term\" : {\"score\":\"80.5\"}}," +
          "       {\"term\" : {\"male\":\"true\"}}" +
          "     ]" +
          "   }" +
          "}" +
        "}";
      ESColumnValueFilter filter = new ESColumnValueFilter(indexNames, httpParametersLong, reqBodyJson);
      scan.setFilter(filter);

      // Submit a scan request.
      rScanner = table.getScanner(scan);

      // Print query results.
      for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
        for (Cell cell : r.rawCells()) {
          LOG.info("testScanDataByPrimaryDataType by multi type condition, scan result:" + Bytes.toString(CellUtil.cloneRow(cell)) + ":"
            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
            + convertCellValue(Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell)));
        }
      }

      LOG.info("Scan data successfully.");
    } catch (IOException e) {
      LOG.error("Scan data failed ", e);
    } finally {
      if (rScanner != null) {
        // Close the scanner object.
        rScanner.close();
      }
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          LOG.error("Close table failed ", e);
        }
      }
    }

    LOG.info("Exiting testScanDataByPrimaryDataType.");
  }

  /**
   * search condition passed by http JSON body with english text keyword
   */
  public void testScanDataByEnglishKeyword() {
    LOG.info("Entering testScanDataByEnglishKeyword.");

    Table table = null;
    // Instantiate a ResultScanner object.
    ResultScanner rScanner = null;
    try {
      // Create the Configuration instance.
      table = conn.getTable(tableName);
      assert table instanceof LemonWrapperHTable;

      // set specified qualifier.
      Scan scan = new Scan();
      scan.addColumn(CF1, QUA_ARTICLE_CONTENT_CHINESE);
      scan.addColumn(CF2, QUA_ARTICLE_CONTENT_English);
      scan.addColumn(CF1, QUA_ARTICLE_ID);

      // Set the cache size.
      scan.setCaching(1000);

      // with special filter
      Map<String, String> httpParameters = Collections.emptyMap();
      List<String> indexNames = new ArrayList();
      indexNames.add(ES_INDEX_NAME);
      // with special filter
      Map<String, String> httpParametersLong = Collections.emptyMap();
      String reqBodyJson =
        "{" +
          "    \"query\" : {" +
          "        \"term\" : { \"contentEng\" : \"game\" }" +
          "    }" +
          "}";
      ESColumnValueFilter filter = new ESColumnValueFilter(indexNames, httpParametersLong, reqBodyJson);
      scan.setFilter(filter);

      // Submit a scan request.
      rScanner = table.getScanner(scan);

      // Print query results.
      for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
        for (Cell cell : r.rawCells()) {
          LOG.info("testScanDataByEnglishKeyword by multi type condition, scan result:" + Bytes.toString(CellUtil.cloneRow(cell)) + ":"
            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
            + convertCellValue(Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell)));
        }
      }

      LOG.info("Scan data successfully.");
    } catch (IOException e) {
      LOG.error("Scan data failed ", e);
    } finally {
      if (rScanner != null) {
        // Close the scanner object.
        rScanner.close();
      }
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          LOG.error("Close table failed ", e);
        }
      }
    }

    LOG.info("Exiting testScanDataByEnglishKeyword.");
  }

  private String convertCellValue(String Qualifier, byte[] value) {
    if (Qualifier.equals(Bytes.toString(QUA_ARTICLE_CONTENT_CHINESE)) ||
        Qualifier.equals(Bytes.toString(QUA_ARTICLE_CONTENT_English))) {
      return Bytes.toString(value);
    } else if (Qualifier.equals(Bytes.toString(QUA_ARTICLE_ID))) {
      return new Long(Bytes.toLong(value)).toString();
    } else if (Qualifier.equals(Bytes.toString(QUA_CHARACTER_NUM))) {
      return new Integer(Bytes.toInt(value)).toString();
    } else if (Qualifier.equals(Bytes.toString(QUA_PAGE_NUM))) {
      return new Short(Bytes.toShort(value)).toString();
    } else if (Qualifier.equals(Bytes.toString(QUA_ARTICLE_LEVEL))) {
      return Bytes.toString(value);
    } else if (Qualifier.equals(Bytes.toString(QUA_RESEARCH_COST))) {
      return new Double(Bytes.toDouble(value)).toString();
    } else if (Qualifier.equals(Bytes.toString(QUA_ARTICLE_SCORE))) {
      return new Float(Bytes.toFloat(value)).toString();
    } else if (Qualifier.equals(Bytes.toString(QUA_AUTHOR_MALE))) {
      return new Boolean(Bytes.toBoolean(value)).toString();
    }
    return "";
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
        // Disable the table before deleting it.
        admin.disableTable(tableName);

        // Delete table.
        admin.deleteTable(tableName);
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
