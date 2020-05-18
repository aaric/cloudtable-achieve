package com.sample.cloudtable;

import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${incarcloud.hbase.table.test}")
    private String hbaseTableTest;

    @Test
    public void testSayHello() {
        log.debug("hbaseTableTest: {}", hbaseTableTest);
    }
}
