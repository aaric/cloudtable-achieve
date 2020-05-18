package com.huawei.cloudtable.hbase.examples.ES;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.security.UserProviderExtend;

import java.io.File;
import java.io.IOException;

public class HBaseESTestMain {
  private final static Log LOG = LogFactory.getLog(HBaseESTestMain.class.getName());
  private static Configuration conf = null;
  // When IAM_AUTH_MODE is true, user and ak and sk must be set.
  private static boolean IAM_AUTH_MODE = false;
  private static String user = "";
  private static String ak = "";
  private static String sk = "";

  public static void main(String[] args) {
    try {
      init();
      if (IAM_AUTH_MODE) {
        // Method login or login2 is OK.
        login(conf);
        //login2(conf);
      }
    } catch (IOException e) {
      LOG.error("Failed to login because ", e);
      return;
    }

    // HBase with ES full text search
    HBaseESSample oneSample;
    try {
      oneSample = new HBaseESSample(conf);
      oneSample.test();
    } catch (Exception e) {
      LOG.error("Failed to test HBase because ", e);
    }
    LOG.info("-----------finish HBase -------------------");
  }

  public static void login(Configuration conf) throws IOException {
    // If IAM_AUTH_MODE is enabled, loginWithAKSK should be called once.
    if (IAM_AUTH_MODE) {
      UserProviderExtend.loginWithAKSK(conf, user, ak, sk);
    }
  }

  public static void login2(Configuration conf) throws IOException {
    // If IAM_AUTH_MODE is enabled, loginWithAKSK should be called once.
    if (IAM_AUTH_MODE) {
      // We can add user and ak and sk to hbase-site.xml
      conf.set("cloudtable.iam.username", user);
      conf.set("cloudtable.iam.accesskey", ak);
      conf.set("cloudtable.iam.secretkey", sk);
      UserProviderExtend.loginWithAKSK(conf);
    }
  }

  private static void init() throws IOException {
    // Default load from conf directory
    conf = HBaseConfiguration.create();
    conf.set(HConnection.HBASE_CLIENT_CONNECTION_IMPL, "org.apache.hadoop.hbase.client.LemonConnectionImplementation");
    String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
    Path hbaseSite = new Path(userdir + "hbase-site.xml");
    if (new File(hbaseSite.toString()).exists()) {
      conf.addResource(hbaseSite);
    }
  }

}
