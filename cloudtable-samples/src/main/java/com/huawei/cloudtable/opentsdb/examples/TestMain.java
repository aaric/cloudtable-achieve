package com.huawei.cloudtable.opentsdb.examples;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;

public class TestMain {
  public static void main(String[] args) {
    OpenTsdbSample openTsdbSample = new OpenTsdbSample();
    try {
      openTsdbSample.put();
      openTsdbSample.query();
      openTsdbSample.delete();
    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
