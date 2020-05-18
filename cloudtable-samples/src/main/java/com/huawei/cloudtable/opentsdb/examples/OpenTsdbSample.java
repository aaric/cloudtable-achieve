package com.huawei.cloudtable.opentsdb.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

public class OpenTsdbSample {
   // TODO : Modify OPENTSDB_IP to IP address and port of your OpenTsdb.
   private final static String OPENTSDB_IP = "";
   private final static int OPENTSDB_PORT = 4242;
   // TODO: If OpenTSDB is security, change securityMode to true,
   // and provide information related security.
   private final static boolean securityMode = false;
   private final static String PROJECT_ID = "";
   private final static String USER = "";
   private final static String AK = "";
   private final static String TOKEN = "";

  private static String PUT_URL = (securityMode ? "https://" : "http://") + OPENTSDB_IP + ":"
      + OPENTSDB_PORT + "/api/put/?sync&sync_timeout=60000";
  private static String QUERY_URL = (securityMode ? "https://" : "http://") + OPENTSDB_IP + ":"
      + OPENTSDB_PORT + "/api/query";

  static class DataPoint {
    public String metric;
    public Long timestamp;
    public Double value;
    public Map<String, String> tags;

    public DataPoint(String metric, Long timestamp, Double value, Map<String, String> tags) {
      this.metric = metric;
      this.timestamp = timestamp;
      this.value = value;
      this.tags = tags;
    }
  }

  static class Query {
    public Long start;
    public Long end;
    public boolean delete = false;
    public List<SubQuery> queries;
  }

  static class SubQuery {
    public String metric;
    public String aggregator;

    public SubQuery(String metric, String aggregator) {
      this.metric = metric;
      this.aggregator = aggregator;
    }
  }

  private String genWeatherData() {
    List<DataPoint> dataPoints = new ArrayList<DataPoint>();
    Map<String, String> tags = ImmutableMap.of("city", "Shenzhen", "region", "Longgang");

    // Data of air temperature
    dataPoints.add(new DataPoint("city.temp", 1498838400L, 28.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498842000L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498845600L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498849200L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498852800L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498856400L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498860000L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498863600L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498867200L, 29.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498870800L, 30.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498874400L, 32.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498878000L, 32.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498881600L, 33.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498885200L, 33.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498888800L, 32.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498892400L, 32.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498896000L, 31.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498899600L, 30.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498903200L, 30.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498906800L, 29.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498910400L, 29.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498914000L, 29.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498917600L, 28.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498921200L, 28.0, tags));

    // Data of humidity
    dataPoints.add(new DataPoint("city.hum", 1498838400L, 54.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498842000L, 53.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498845600L, 52.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498849200L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498852800L, 50.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498856400L, 49.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498860000L, 48.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498863600L, 46.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498867200L, 46.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498870800L, 48.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498874400L, 48.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498878000L, 49.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498881600L, 49.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498885200L, 50.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498888800L, 50.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498892400L, 50.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498896000L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498899600L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498903200L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498906800L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498910400L, 52.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498914000L, 53.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498917600L, 54.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498921200L, 54.0, tags));

    Gson gson = new Gson();
    return gson.toJson(dataPoints);
  }

  String genQueryReq() {
    Query query = new Query();
    query.start = 1498838400L;
    query.end = 1498921200L;
    query.queries = ImmutableList.of(new SubQuery("city.temp", "sum"),
        new SubQuery("city.hum", "sum"));

    Gson gson = new Gson();
    return gson.toJson(query);
  }

  String genDeleteReq() {
    Query query = new Query();
    query.start = 1498838400L;
    query.end = 1498921200L;
    query.queries = ImmutableList.of(new SubQuery("city.temp", "sum"),
        new SubQuery("city.hum", "sum"));
    query.delete = true;

    Gson gson = new Gson();
    return gson.toJson(query);
  }

  public void put() throws ClientProtocolException, IOException {
    try (CloseableHttpClient httpClient = OpenTsdbUtils.createHttpOrHttpsClient(securityMode)) {
      HttpPost httpPost = new HttpPost(PUT_URL);
      addSecurityHeader(httpPost);
      addTimeout(httpPost);
      String weatherData = genWeatherData();
      StringEntity entity = new StringEntity(weatherData, "utf-8");
      entity.setContentType("application/json");
      httpPost.setEntity(entity);
      HttpResponse response = httpClient.execute(httpPost);

      int statusCode = response.getStatusLine().getStatusCode();
      System.out.println("Status Code : " + statusCode);
      if (statusCode != HttpStatus.SC_NO_CONTENT) {
        System.out.println("Request failed! " + response.getStatusLine());
      }
    }
  }

  public void query() throws ClientProtocolException, IOException {
    try (CloseableHttpClient httpClient = OpenTsdbUtils.createHttpOrHttpsClient(securityMode)) {
      HttpPost httpPost = new HttpPost(QUERY_URL);
      addSecurityHeader(httpPost);
      addTimeout(httpPost);
      String queryRequest = genQueryReq();
      // System.out.println("Request=" + queryRequest);
      StringEntity entity = new StringEntity(queryRequest, "utf-8");
      entity.setContentType("application/json");
      httpPost.setEntity(entity);
      HttpResponse response = httpClient.execute(httpPost);

      int statusCode = response.getStatusLine().getStatusCode();
      System.out.println("Status Code : " + statusCode);
      if (statusCode != HttpStatus.SC_OK) {
        System.out.println("Request failed! " + response.getStatusLine());
      }

      String body = EntityUtils.toString(response.getEntity(), "utf-8");
      System.out.println("Response content : " + body);
    }
  }

  public void delete() throws ClientProtocolException, IOException {
    try (CloseableHttpClient httpClient = OpenTsdbUtils.createHttpOrHttpsClient(securityMode)) {
      HttpPost httpPost = new HttpPost(QUERY_URL);
      addSecurityHeader(httpPost);
      addTimeout(httpPost);
      String deleteRequest = genDeleteReq();
      StringEntity entity = new StringEntity(deleteRequest, "utf-8");
      entity.setContentType("application/json");
      httpPost.setEntity(entity);
      HttpResponse response = httpClient.execute(httpPost);

      int statusCode = response.getStatusLine().getStatusCode();
      System.out.println("Status Code : " + statusCode);
      if (statusCode != HttpStatus.SC_OK) {
        System.out.println("Request failed! " + response.getStatusLine());
      }
    }
  }

  public static void addSecurityHeader(HttpRequestBase req) {
    if (securityMode) {
      req.addHeader("X-TSD-IamAuth", "true");
      req.addHeader("X-Auth-ProjectId", PROJECT_ID);
      req.addHeader("X-Auth-User", USER);
      req.addHeader("X-Auth-AK", AK);
      req.addHeader("X-Auth-Token", TOKEN);
    }
  }

  public static void addTimeout(HttpRequestBase req) {
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
        .setConnectionRequestTimeout(10000).setSocketTimeout(60000).build();
    req.setConfig(requestConfig);
  }
}
