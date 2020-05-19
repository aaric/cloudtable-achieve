# cloudtable-achieve

[![license](https://img.shields.io/badge/license-MIT-green.svg?style=flat&logo=github)](https://www.mit-license.org)
[![java](https://img.shields.io/badge/java-1.8-brightgreen.svg?style=flat&logo=java)](https://www.oracle.com/java/technologies/javase-downloads.html)
[![gradle](https://img.shields.io/badge/gradle-6.3-brightgreen.svg?style=flat&logo=gradle)](https://docs.gradle.org/6.3/userguide/installation.html)
[![build](https://github.com/aaric/cloudtable-achieve/workflows/build/badge.svg)](https://github.com/aaric/cloudtable-achieve/actions)
[![release](https://img.shields.io/badge/release-0.2.0-blue.svg)](https://github.com/aaric/cloudtable-achieve/releases)

> Huawei CloudTable Learning. -- [hadoop version support matrix](http://hbase.apache.org/book.html#hadoop)

## 一键安装CloudTable的HBase客户端

> [华为云官方参考文档。](https://support.huaweicloud.com/usermanual-cloudtable/cloudtable_01_0097.html)

```bash
# su - root
sh> curl -O -k "http://cloudtable-publish.obs.myhwclouds.com/quick_start_hbase_shell.sh"
# $zookeeper_address=zk1:2181,zk2:2181,zk3:2181
sh> source quick_start_hbase_shell.sh $zookeeper_address
sh> hbase shell
```
