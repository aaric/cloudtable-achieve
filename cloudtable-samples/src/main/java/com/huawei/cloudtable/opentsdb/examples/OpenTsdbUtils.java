package com.huawei.cloudtable.opentsdb.examples;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

public class OpenTsdbUtils {

  public static CloseableHttpClient createHttpOrHttpsClient(boolean securityMode) {
    if (securityMode) {
      return createSSLClientDefault();
    } else {
      return HttpClients.createDefault();
    }
  }

  private static CloseableHttpClient createSSLClientDefault() {
    try {
      X509TrustManager x509mgr = new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] xcs, String string) {
        }

        public void checkServerTrusted(X509Certificate[] xcs, String string) {
        }

        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }
      };
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, new TrustManager[] { x509mgr }, null);
      @SuppressWarnings("deprecation")
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
          SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

      return HttpClients.custom().setSSLSocketFactory(sslsf).build();
    } catch (KeyManagementException e) {
      throw new RuntimeException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
