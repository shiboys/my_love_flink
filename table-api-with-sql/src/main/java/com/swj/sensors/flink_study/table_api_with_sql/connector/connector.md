# 第一次使用 Kafka Connector 耗时半天复盘
第一次使用 Kafka Connector 代码参考了
https://gitee.com/geektime-geekbang/geektime-Flink/blob/master/lab-5-table-api-with-sql/src/main/java/com/geekbang/flink/kafka/KafkaTableConnector.java
然后 我看了下 这个源码，发现这个 schema 定义特别恶心人，不怎么想用 schema，就查了 flink 1.11.1 的官方文档和其他文档，最后参考了微软的一个开发文档 https://learn.microsoft.com/en-us/azure/hdinsight-aks/flink/create-kafka-table-flink-kafka-sql-connector
发现这个文档写的比较牛逼，通过简单的 python 文件 和 shell 命令就能向 kafka 发生数据，然后创建 Flink SQL 的 table

后半部分的 sink to csv 还是参考了极客时间的代码

代码昨天晚上写完，部署一把，提交任务失败，抛出异常，回去睡觉

今天早上来了，继续调试，然后就是痛苦的开始。。。。

费了整整一个上午，才把所有问题解决，调试通过，数据成功从 kafak 写入 CSV 文件

## 问题一：不识别 metadata 关键字

根据 微软的那篇文章，发现 flink 1.11.1 提取 timestamp 不能用 metadata 关键字，查了 flink 1.11.1 的文档，发现没有好的办法直接提取，就直接干脆不提取了，改 SQL 语句，用 ts 的原始 string 类型。

## 问题二：sql 语法报错  Encountered "\'\n) WITH (\n \'

这个问题看了半天，不知道哪错了，坑呀！！！ 在 WITH 关键字上，左空格右空格左右空格倒腾了半天，搞不定，后来发现是 `ts` STRING' 后面多了一个 单引号。主要是 with 里面单引号太多

```java
 String ddlSql = "CREATE TABLE kafka_click_event_table (\n"
        + "`userName` STRING,\n"
        + "`visitUrl` STRING, \n"
        + "`ts` STRING' \n" // 从 kafka 的元数据中读取 timestamp 信息，这个要 1.13 版本才可以，目前我是 1.11 版本，就继续改用 string
        + ") WITH (\n";
```

### 问题三，微软版本的 Flink Sql 语法至少是 1.13 的，跟 1.11 的差别还是很多

下面给出 微软版本的 flink sql 和 1.11.1 经过调试后 无异常的 sql
经过 对比就会发现 with 的语法发生的了很大变化：connector 的关键字前面都需要加 connector 前缀，这个确实恶心，所以后来版本的 flink sql 给优化后省略了。
但是，我当前用的是 flink 1.11.1。而我本机部署的另外一套 flink 的最新版本是 1.18，用 1.18 提交作业会提示 用 BlinkPlanner 的配置错误，如下所示，没办法继续硬着头皮将所有的 with 后面的关键子都加上 connector. 的前缀

```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
```



```sql
CREATE TABLE KafkaTable (
`userName` STRING,
`visitURL` STRING,
`ts` TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
'connector' = 'kafka',
'topic' = 'click_events',
'properties.bootstrap.servers' = '<update-kafka-bootstrapserver-ip>:9092,<update-kafka-bootstrapserver-ip>:9092,<update-kafka-bootstrapserver-ip>:9092',
'properties.group.id' = 'my_group',
'scan.startup.mode' = 'earliest-offset',
'format' = 'json'
);
```
Flink sql 1.11.1 版本的 sql :

```java
 String ddlSql = "CREATE TABLE kafka_click_event_table (\n"
        + "`userName` STRING,\n"
        + "`visitUrl` STRING, \n"
        + "`ts` STRING \n" // 从 kafka 的元数据中读取 timestamp 信息，这个要 1.13 版本才可以，目前我是 1.11 版本，就继续改用 string
        + ") WITH (\n"
        + " 'connector.type' = 'kafka', \n"
        + " 'connector.version' = '0.11', \n" // kafka 的版本，Flink附带了提供了多个Kafka连接器：universal通用版本，0.10，0.11，官方文档解释说universal(通用版本)的连接器，会尝试跟踪Kafka最新版本，兼容0.10或者之后的Kafka版本，官方文档也说对于绝大多数情况使用这个即可
        + " 'connector.topic' = 'click_events', \n"
        + " 'connector.properties.bootstrap.servers' = 'localhost:9092', \n"
        + " 'connector.properties.group.id' = 'kafka-table-connector-group-1', \n"
        + " 'connector.startup-mode' = 'earliest-offset', \n"
        + " 'update-mode' = 'append', \n" // declare update mode
        + " 'format.type' = 'json' \n"
        + " )";
```

### 问题四：Required context properties mismatch 异常

异常信息如下：

```java
Caused by: org.apache.flink.table.api.NoMatchingTableFactoryException: Could not find a suitable table factory for 'org.apache.flink.table.factories.TableSourceFactory' in
the classpath.

Reason: Required context properties mismatch.

The following properties are requested:
connector.properties.bootstrap.servers=localhost:9092
...

The following factories have been considered:
org.apache.flink.table.sources.CsvBatchTableSourceFactory
org.apache.flink.table.sources.CsvAppendTableSourceFactory
org.apache.flink.table.filesystem.FileSystemTableFactory
	at org.apache.flink.table.factories.TableFactoryService.filterByContext(TableFactoryService.java:322)
	at org.apache.flink.table.factories.TableFactoryService.filter(TableFactoryService.java:190)
	at org.apache.flink.table.factories.TableFactoryService.findSingleInternal(TableFactoryService.java:143)
	at org.apache.flink.table.factories.TableFactoryService.find(TableFactoryService.java:96)
	at org.apache.flink.table.factories.TableFactoryUtil.findAndCreateTableSource(TableFactoryUtil.java:46)
	... 33 more
```
纳尼？没办法，继续google 把，查了半天，这个问题的解法五花八门，有的说把 `connector.version=universe` 试了下不管用；根据错误说明再根据极客时间的源码 `tableEnv.inAppendMode().createTemporaryTable("order_table");` 然后再查 Flink Sql 1.11.1 的官方文档
https://nightlies.apache.org/flink/flink-docs-release-1.11/dev/table/connect.html#update-mode
这篇文章指出，我没有配置 update-mode,没办法，配置上去，重新编译、打包、提交任务(因为 微软的文章就没有配置 update-mode)


### 问题五：'connector.type' expects 'filesystem', but is 'kafka'

配置上 update-mode 之后，开始继续报 `Required context properties mismatch.` 这种类型的错误，只是错误的内容变了，如下所示

```java
Reason: Required context properties mismatch.

The matching candidates:
org.apache.flink.table.sources.CsvAppendTableSourceFactory
Mismatched properties:
'connector.type' expects 'filesystem', but is 'kafka'
'format.type' expects 'csv', but is 'json'

The following properties are requested:
update-mode=append

The following factories have been considered:
org.apache.flink.table.sources.CsvBatchTableSourceFactory
org.apache.flink.table.sources.CsvAppendTableSourceFactory
org.apache.flink.table.filesystem.FileSystemTableFactory
	at org.apache.flink.table.factories.TableFactoryService.filterByContext(TableFactoryService.java:322)
	at org.apache.flink.table.factories.TableFactoryService.filter(TableFactoryService.java:190)
	at org.apache.flink.table.factories.TableFactoryService.findSingleInternal(TableFactoryService.java:143)
	at org.apache.flink.table.factories.TableFactoryService.find(TableFactoryService.java:96)
	at org.apache.flink.table.factories.TableFactoryUtil.findAndCreateTableSource(TableFactoryUtil.java:46)
	... 33 more

```

继续 google ，发现说是 没有 flink-connector-kafka 的 jar 包，可是我明明配置了呀，不知道怎么回事，先根据网上的解法，将我现在用的版本对应的 flink-connector-kafka-0.11_2.11-1.11.1.jar 拷贝到 $FLINK_HOME/lib 目录下。

重新提交任务，发现继续报错，`java.lang.ClassNotFoundException: org.apache.flink.streaming.connectors.kafka.KafkaTableSourceSinkFactoryBase`

没办法，继续将 flink-connector-kafka-0.11_2.11-1.11.1 所依赖的 flink-connector-kafka-0.10_2.11-1.11.1.jar 和 flink-connector-kafka-base_2.11-1.11.1.jar 拷贝到 $FLINK_HOME/lib 下 
此问题得以解决

### 问题六 Unsupported property keys: connector.startup.mode

问题如下：

```java

Reason: No factory supports all properties.

The matching candidates:
org.apache.flink.streaming.connectors.kafka.Kafka011TableSourceSinkFactory
Unsupported property keys:
connector.startup.mode
```

这个问题的描述已经很详细了，`connector.startup.mode` 这个关键字的不支持，查看官方文档，发现是 `'connector.startup-mode' = 'earliest-offset' ` 而 `connector.startup.mode` 这个我是根据微软的那篇文章来写的配置，低版本的 flink sql 肯定不能识别高版本的语法，改掉 然后解决

### 问题七 request schema 不匹配 sink schema

```java
Query schema: [userName: VARCHAR(2147483647), visitUrl: VARCHAR(2147483647), ts: VARCHAR(2147483647)]
Sink schema: [userName: VARCHAR(2147483647), visitUrl: VARCHAR(2147483647), ts: TIMESTAMP(3)]
```
既然 timestamp 暂时得不到有效解决，就把 sink 的类型改掉
```java
tEnv.registerTableSink("result_table",
    // specify the table schema
    new String[] {"userName", "visitUrl", "ts"},
    new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING},// 此处将 Types.SQL_TIMESTAMP 改为 Types.STRING
    sink);
```

### 问题八：java.lang.NoClassDefFoundError: org/apache/kafka/common/serialization/ByteArrayDeserializer

又是这个 NoClassDefFoundError ，我都快疯了，继续 google 查询，发现说是缺少 `kafka-clients-0.11.0.2.jar` 这个 jar 包，可是我看 flink-connector-kafka 都已经包含这个包了，真是坑呀，没办法，先继续将其拷贝到 $FLINK_HOME/bin 下
最终运行成功。

## 总结：

这 8 个问题解决之后，我没有一点成就感，难道说用一个稍微老一点的 Flink 版本 就学习 Flink Sql 这么困难吗？

Flink 的 fat 包就不能包含这些 缺失的 jar 吗？




