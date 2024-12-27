
DROP TABLE IF EXISTS user_behavior;

/*
用户行为表，对应 kafka 中的 数据
*/
CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    event_ts BIGINT,
    ts AS TO_TIMESTAMP(FROM_UNIXTIME(event_ts, 'yyyy-MM-dd HH:mm:ss')), -- 定义事件时间
    proctime as PROCTIME(),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'user_behavior',
    'connector.startup-mode' = 'earliest-offset',
    'connector.properties.zookeeper.connect' = 'zookeeper:2181',
    'connector.properties.bootstrap.servers' = '172.18.0.7:9092',
    'update-mode' = 'append',
    'format.type' = 'json'
);

/*
创建第一个表，用来满足第一个需求，统计每小时的成交量
*/

CREATE TABLE buy_cnt_per_hour (
    hour_of_day BIGINT, 
    buy_cnt BIGINT
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '7',
    'connector.hosts' = 'http://127.0.0.1:9200',
    'connector.index' = 'buy_cnt_per_hour',
    'connector.document-type' = 'user_behavior',
    'connector.bulk-flush.max-actions' = '1',
    'format.type' = 'json',
    'update-mode' = 'append'
);

/*
接下来就是 flink sql 的作业 sql，通过查询 kafka 表中的数据，实时进行统计，
并写入 es 中
*/

INSERT INTO buy_cnt_per_hour
SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);


/*
第二个需求：
每 10 分钟统计 1 天累计独立用户数
这个其实用 accumulator 窗口能实现，但是这里的实现是用的是 OverWindow
*/

/*
ROWS OVER Window 按照实际元素的行确定窗口。本例还没有 partition by 进行分区
RANGE OVER Window 按照实际的元素值
PRIMARY KEY (date_str, time_str) NOT ENFORCED 是指：
Elasticsearch结果表会根据是否定义了主键，确定是在upsert模式或append模式下工作。

如果定义了主键，则主键必须为文档ID，Elasticsearch结果表将在upsert模式下工作，该模式可以消费包含UPDATE和DELETE的消息。

如果未定义主键，Elasticsearch将自动生成随机的文档ID，Elasticsearch结果表将在append模式工作，该模式只能消费INSERT消息。
*/

CREATE TABLE cumalative_uv (
    date_str STRING,
    time_str STRING,
    uv BIGINT,
    PRIMARY KEY (date_str, time_str) NOT ENFORCED
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '7',
    'connector.hosts' = 'http://localhost:9200',
    'connector.index' = 'cumulative_uv',
    'connector.document-type' = 'user_behavior',
    'format.type' = 'json',
    'update-mode' = 'upsert'
);

/*
UNBOUNDED PRECEDING表示整个数据流的开始作为起始点，也可以使用rowCount PRECEDING来表示当前行之前的某个元素作为起始点，
rowCount是一个数字；CURRENT ROW表示当前行作为结束点。
产生的数据是 01:0 ,cnt; 01:1, cnt 这样的数据，取分钟的 10 位数达到 10 分钟统计一次。然后
SUBSTR(DATE_FORMAT(ts, 'HH:MM'), 1,4) || '0' 也将分钟的个位数抹去之后再添加'0' 最终形成 01:00, 01:10 这样子的每隔 10 分钟的格式
我怎么看这个语句都无法满足 每 10 分钟统计 1 天累计独立用户数 ，后来我去 flink 官方网站找到这个示例，就是 吴翀 写的
https://flink.apache.org/2020/07/28/flink-sql-demo-building-an-end-to-end-streaming-application/
才发现 极客时间 的是错的，而且错的还不少，坑死人。

*/
CREATE VIEW view_uv_per_10min AS
SELECT
DATE_FORMAT(ts,'yyyy-MM-dd') AS date_str,
SUBSTR(DATE_FORMAT(ts, 'HH:MM'), 1,4) || '0' AS time_str,
user_id 
FROM user_behavior ;

/*
提交第二个作业
这个作业之所以会准确统计每 10 分钟的 uv 数据
是因为 cumalative_uv 有 date_str:time_str 的主键，且 es 处于 upsert 模式下
对于新的流式 UV 数据，同一天，同一小时 同一 10 分钟的数据，如果主键冲突，则更新 UV ，符合要求
如果不存在主键，这插入数据，也符合要求，这样就达到了 10 分钟的窗口数据可能会更改，
但是这里不是 每隔 10 分钟才更新一次，而是 来一条数据，就会更新记录，而每隔 10 分钟就是 tumble 窗口了。
根据官方的需求描述 ：
Another interesting visualization is the cumulative number of unique visitors (UV). For example, 
the number of UV at 10:00 represents the total number of UV from 00:00 to 10:00. 
Therefore, the curve is monotonically increasing.
max(time_str) 这个能满足当前统计的时间单位，date_str 满足统计的日期单位
*/

INSERT INTO cumalative_uv
SELECT date_str, max(time_str), COUNT(DISTINCT user_id) AS uv
FROM view_uv_per_10min
GROUP BY date_str;

/*
第 3 个需求最复杂
TopN 类目排行榜
共 分为 4 大步骤：
1、创建 base 在 mysql 的类目维表
2、创建 sink 在 elasticsearch 的统计结果持久化表
3、创建一个 带有 类目中文名称的 rich_user_behavior 的视图
4、对第三步视图的结果 group by 类目名称，count(*) 的结果 写入 第二步的持久化表中
*/

/*
创建 基于 mysql 的 flink table 之前，要确保 mysql 的目标数据库和目标表存在，相关的创建语句和数据初始化脚本
请参考 project-sql.md 文档。
另外驱动用的是 mysql 8.0 的驱动，对应的 jar 包也应该是 mysql 8.0 的
*/
CREATE TABLE category_dim(
    sub_category_id BIGINT,
    parent_category_id BIGINT
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink',
    'connector.table' = 'category',
    'connector.driver' = 'com.mysql.cj.jdbc.Driver',
    'connector.username' = 'flink',
    'connector.password' = '123456',
    'connector.lookup.cache.max-rows' = '5000',
    'connector.lookup.cache.ttl' = '10min'
);

/*
创建 sink 到 ES 的 flink table。
类型（Document Type）
在 Elasticsearch 7.0 之前，一个 Index 可以创建多个 Document Type，但在 7.0 开始及之后，一个Index 只能对应一个 Document Type，且默认是 _doc 。
ach document indexed is associated with a _type and an _id. The _type field is indexed in order to make searching by type name fast
*/
CREATE TABLE top_category(
    category_name STRING,
    buy_cnt BIGINT
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '7',
    'connector.hosts' = 'http://localhost:9200',
    'connector.index' = 'top_category',
    'connector.document-type' = 'user_hehavior',
    'format.type' = 'json',
    'update-mode' = 'upsert'
);

/*
第三步，创建一个 rich_user_behavior 的视图，将 user_behavior 的 category_id 跟 category_name 对应起来
这里面用到了一个 Flink Sql 的 join 新语法：for system_time as of，关于这个语法的描述，请参考 project-sql.md 文档
*/

CREATE VIEW view_rich_user_behavior AS
SELECT u.user_id,u.item_id, u.behavior,
CASE c.parent_category_id
    WHEN 0 THEN '服饰鞋包'
    WHEN 1 THEN '家装家饰'
    WHEN 2 THEN '家电'
    WHEN 3 THEN '美妆'
    WHEN 4 THEN '母婴'
    WHEN 5 THEN '3C数码'
    WHEN 6 THEN '运动户外'
    WHEN 7 THEN '食品'
    WHEN 8 THEN '医药'
    ELSE '其他'
END AS category_name
FROM user_behavior AS u LEFT JOIN category_dim FOR SYSTEM_TIME AS OF u.proctime AS c
ON u.category_id = c.sub_category_id;


/*
最后执行生成任务
*/

INSERT INTO top_category
SELECT category_name, COUNT(*) AS buy_cnt
FROM view_rich_user_behavior
WHERE behavior = 'buy'
GROUP BY category_name;