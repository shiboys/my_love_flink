# Hadoop 环境搭建
Mac 环境的搭建，可以参考收藏夹中的 hadoop mac 环境的搭建，总体来说比较简单，我从官网下载的 hadoop 3.4.0 的 binary 压缩包
解压，修改配置，添加环境变量，就能在本机上跑 hadoop 了。


# Flink 配置 checkpoint 路径为 hdfs 的路径 疑难杂张

## 一、主要是 classNotFound

```text
Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Cannot support file system for 'hdfs' via Hadoop, because Hadoop is not in the classpath, or some classes are missing from the classpath.
	at org.apache.flink.runtime.fs.hdfs.HadoopFsFactory.create(HadoopFsFactory.java:189)
	at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:526)
	... 23 more
Caused by: java.lang.NoSuchMethodError: org.apache.hadoop.fs.FsTracer.get(Lorg/apache/hadoop/conf/Configuration;)Lorg/apache/htrace/core/Tracer;
	at org.apache.hadoop.hdfs.DFSClient.<init>(DFSClient.java:307)
	at org.apache.hadoop.hdfs.DFSClient.<init>(DFSClient.java:292)
	at org.apache.hadoop.hdfs.DistributedFileSystem.initDFSClient(DistributedFileSystem.java:200)
	at org.apache.hadoop.hdfs.DistributedFileSystem.initialize(DistributedFileSystem.java:185)
	at org.apache.flink.runtime.fs.hdfs.HadoopFsFactory.create(HadoopFsFactory.java:168)
```
这个问题我在网上找了好久，最终还是解决了。主要是通过配置 hadoop 的classapth 到 /etc/profile
```shell
# 本机的 Hadoop 地址
export HADOOP_HOME=/usr/local/Cellar/hadoop/hadoop-3.4.0
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_INSTALLL=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CLASSPATH=`hadoop classpath`
```
最重要的是要 source /etc/profile 使之生效，但是我在这里栽了一个大跟头，我 source 之后，提交 flink job，
一直报 classNotFound 的异常而不解，我就找呀找，还有网站说下载 `commons-cli-1.5.0.jar` 和 `flink-shaded-hadoop-3-3.1.1.7.2.8.0-224-9.0.jar`
我都弄了，提交一直报错。后来我发现是因为我的 Flink standonly 模式启动之后，我才使用 source 命令将 hadoop home
工作目录生效，一定要在 Flink 框架启动之前，将 hadoop 的相关 classpath 生效，这样 flink 就能正确地加载 hadoop 相关的包了, ~~「而 pom 文件中加载的
hadoop 相关包起始用处不是很大，如果没有使用 hadoop 相关的 api 的话，可以不用加载』~~。随后我在本机实验证明，缺少了pom 中的相关 jar 包，flink job 无法提交成功
仍然提示缺少包，看起来必须要在 pom 文件中加载 hadoop 相关的包。

## 从 SavePoint 中重启

```shell

bin/flink savepoint a5e894b29fdee8a65cb79cb537f9c332 hdfs://localhost:8020/flink-cdc-simple1/savepoints
Triggering savepoint for job a5e894b29fdee8a65cb79cb537f9c332.
Waiting for response...
Savepoint completed. Path: hdfs://localhost:8020/flink-cdc-simple1/savepoints/savepoint-a5e894-01016713ef86

```
```shell
bin/flink run -s hdfs://localhost:8020/flink-cdc-simple1/savepoints/savepoint-a5e894-01016713ef86 -c com.swj.sensors.flink_study.FlinkCdcMysqlSourceDemo lib/flink-18-1.0-SNAPSHOT.jar
```





