package com.sample.cloudtable.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

/**
 * CloudTable配置
 *
 * @author Aaric, created on 2020-05-18T13:38.
 * @version 0.1.0-SNAPSHOT
 */
public class CloudTableConfig {

    @Value("${incarcloud.hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${incarcloud.hbase.zookeeper.property.clientPort}")
    private String zookeeperClientPort;

    @Value("${incarcloud.hbase.master}")
    private String hbaseMaster;

    @Bean
    public Connection hbaseConnection() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
        configuration.set("hbase.master", hbaseMaster);

        return ConnectionFactory.createConnection(configuration);
    }
}
