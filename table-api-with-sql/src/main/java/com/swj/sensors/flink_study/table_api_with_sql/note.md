# FlinkSQL 基本概念、时间属性和窗口


## 时间属性

Flink 可以基于几种不同的 *时间* 概念来处理数据。

* *处理时间* 指的是执行具体操作的机器时间(大家熟知的绝对时间，例如 Java 的 System.currentTimeMillis())
* *事件时间* 指的是数据本身携带的时间。这个时间是在事件产生时的时间
* *摄入时间* (Ingesting Time) 指的是数据进入 Flink 的时间；在系统内部，会把它当做事件时间来处理。

## 时间属性介绍
___

像窗口(Table API 和 SQL 中的窗口)这种基于时间的操作，需要有时间信息。因此，Table API 中的表就需要提供逻辑时间属性来表示时间，以及支持时间相关的操作。

每种类型的表都可以有时间属性，可以在 CREATE TABLE DDL 创建表的时候指定，也可以在 DataStream 中指定、也可以在定义 TableSource 时指定。一旦时间属性定义好，它就可以像普通列一样使用，也可以在时间相关的操作中使用。

只要时间属性没有被修改，而是简单从一个表传递到另一个表，它就仍然是一个有效的时间属性。时间属性可以像普通的时间戳的列一样被使用和计算。一旦时间属性被用在了了计算中，它就会被物化，进而变成一个普通的时间戳。普通的时间戳时无法跟 Flink 的时间以及 watermark 等一起使用的，所以普通的时间戳也就无法用在时间相关的操作中。

Table API 程序需要在 Stream Environment 中指定时间属性：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // default
```

### 事件时间
___
事件时间允许程序按照数据中包含的时间类处理，这样可以在有乱序或者晚到的数据的情况下产生一致的处理结果。它可以保证从外部存储读取数据后产生可以复现(replayable)的结果。

除此之外，事件时间可以程序在流式和批处理作业中使用同样的语法。在流式程序中的事件时间属性，在批式程序中就是一个正常的时间字段。

为了能够处理乱序的事件，并且区分正常到达和晚到的事件，Flink 需要从事件中获取时间并且产生 watermark。

事件时间属性也有类似于处理时间的三中定义方式：在 DDL 中定义，在 DataStream 到 Table 的转换时定义，用 TableSource 定义。

#### 在 DDL 中定义

事件时间属性可以用 WATERMARK 语句在 CREATE TABLE DDL 中进行定义。WATERMAEK 语句在一个已有字段上定义一个 watermark 生成表达式，同时标记这个已有字段为时间属性字段。

Flink 支持在 TIMESTAMP 列上定义事件时间。如果数据源中的时间戳数据表示为年-月-日-时-分-秒，则通常为不带时区信息的字符串值，例如 2020-04-15 20:13:40.564，建议将事件时间属性定义在 TIMESTAMP 列上:

```sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time TIMESTAMP(3),
  -- 声明 user_action_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
```

数据源中的时间戳表示为一个机缘(epoch) 时间，通常是一个 long 值，例如 1618989564564，建议将事件时间属性定义在 TIMESTAMP_LTZ 列上：

```sql
CREATE TABLE user_actions (
 user_name STRING,
 data STRING,
 ts BIGINT,
 time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
 -- declare time_ltz as event time attribute and use 5 seconds delayed watermark strategy
 WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND
) WITH (
 ...
);

SELECT TUMBLE_START(time_ltz, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(time_ltz, INTERVAL '10' MINUTE);
```

#### DataStream 到 Table 转换时定义。

事件时间属性可以用 **.rowtime 后缀** 在定义 DataStream schema 的时候来定义。时间戳和 watermark 在这之前一定是在 DataStream 上已经定义好了。在从 DataStream 转换到 Table时，由于 DataStream 没有时区的概念，因此 Flink 总是将 rowtime 属性解析成 TIMESTAMP WITHOUT TIME ZONE 类型，并且将所有的事件时间的值都视为 UTC 时区的值。

再从 DataStream 到 Table 转换时定义事件时间属性有两种方式。取决于 .rowtime 后缀修饰的字段名是否已经有字段，事件时间字段可以是：
* 在 schema 的结尾追加一个新的字段
* 替换一个已经存在的字段

不管在哪种情况下，事件时间字段都表示 DataStream 中定义的事件的时间戳。

```java
// Option 1:

// 基于 stream 中的事件产生时间戳和 watermark
DataStream<Tuple2<String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// 声明一个额外的逻辑字段作为事件时间属性
Table table = tEnv.fromDataStream(stream, $("user_name"), $("data"), $("user_action_time").rowtime());


// Option 2:

// 从第一个字段获取事件时间，并且产生 watermark
DataStream<Tuple3<Long, String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// 第一个字段已经用作事件时间抽取了，不用再用一个新字段来表示事件时间了
Table table = tEnv.fromDataStream(stream, $("user_action_time").rowtime(), $("user_name"), $("data"));

// Usage:

WindowedTable windowedTable = table.window(Tumble
       .over(lit(10).minutes())
       .on($("user_action_time"))
       .as("userActionWindow"));
```

#### 使用 TableSource 定义

事件时间属性可以在实现了 DefinedRowTimeAttribute 的 TableSource 中定义。 getRowTimeAttributeDescriptors() 方法返回 RowTimeAttributeDescriptor 的列表，包含了描述事件时间属性的字段名字，如何计算事件时间、以及 watermark 生成策略等信息。

同时需要确保 getDataStream 返回的 DataStream 以及定义好了时间属性。只有在定义了 StreamRecordTimeStamp 时间戳分配器的时候，才认为 DataStream 是有时间戳信息的。只有定义了 PreserveWatermarks watermark 生成策略的 DataStream 的 watermark 才会被保留。反之，则只有时间字段是生效的。

```java
// 定义一个有事件时间属性的 table source
public class UserActionSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"user_name", "data", "user_action_time"};
		TypeInformation[] types =
		    new TypeInformation[] {Types.STRING(), Types.STRING(), Types.LONG()};
		return Types.ROW(names, types);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		// 构造 DataStream
		// ...
		// 基于 "user_action_time" 定义 watermark
		DataStream<Row> stream = inputStream.assignTimestampsAndWatermarks(...);
		return stream;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		// 标记 "user_action_time" 字段是事件时间字段
		// 给 "user_action_time" 构造一个时间属性描述符
		RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
			"user_action_time",
			new ExistingField("user_action_time"),
			new AscendingTimestamps());
		List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
		return listRowtimeAttrDescr;
	}
}

// register the table source
tEnv.registerTableSource("user_actions", new UserActionSource());

WindowedTable windowedTable = tEnv
	.from("user_actions")
	.window(Tumble.over(lit(10).minutes()).on($("user_action_time")).as("userActionWindow"));
```

### 处理时间
___
处理时间是基于及其的本地时间来处理数据，它是最简单的一种时间概念，但是它不能提供确定性。它既不需要从数据中获取时间，也不需要生成 watermark。

共有三种方法可以定义处理时间。

#### 在创建表的 DDL 中定义。

处理时间属性可以在创建表的 DDL 中用计算列的方式定义，用 PROCTIME() 就可以定义处理时间，函数 PROCTIME() 的返回类型是 TIMESTAMP_LTZ.
所谓的计算列是 Flink SQL 中引入的特殊概念，可以用一个 AS 语句来在表中产生书怒中不存在的列，并且可以利用原有的列，各种运算符以及内置函数。在前面时间时间属性定义中，将 ts 字段转换成 TIMESTAMPE_LTZ(local time zone) 类型的 time_ltz，也是计算列的定义方式。这一点类似于 clickhouse 的语法。

```sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time AS PROCTIME() -- 声明一个额外的列作为处理时间属性
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
```

处理时间属性可以在 schema 定义的时候用 **.proctime 后缀**来定义。**时间属性一定不能定义在一个已有字段上，所以它只能定义在 schema 定义的最后**。

```java
DataStream<Tuple2<String, String>> stream = ...;

// 声明一个额外的字段作为时间属性字段
Table table = tEnv.fromDataStream(stream, $("user_name"), $("data"), $("user_action_time").proctime());

WindowedTable windowedTable = table.window(
        Tumble.over(lit(10).minutes())
            .on($("user_action_time"))
            .as("userActionWindow"));
```

#### 使用 TableSource 定义

处理时间属性可以在实现了 DefinedProctimeAttribute 的 TableSource 中定义。逻辑的时间属性会放在 TableSource 已有的物理字段的最后。

```java
// 定义一个由处理时间属性的 table source
public class UserActionSource implements StreamTableSource<Row>, DefinedProctimeAttribute {

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"user_name" , "data"};
		TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING()};
		return Types.ROW(names, types);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		// create stream
		DataStream<Row> stream = ...;
		return stream;
	}

	@Override
	public String getProctimeAttribute() {
		// 这个名字的列会被追加到最后，作为第三列
		return "user_action_time";
	}
}

// register table source
tEnv.registerTableSource("user_actions", new UserActionSource());

WindowedTable windowedTable = tEnv
	.from("user_actions")
	.window(Tumble
	    .over(lit(10).minutes())
	    .on($("user_action_time"))
	    .as("userActionWindow"));
```

## 窗口

有了时间属性，接下来就可以定义窗口进行计算了。我们知道，窗口可以将无界流切割成大小有限的“桶”(bucket) 来计算，通过截取有限数据集来处理无限的流数据。在 DataStream API 中提供了对不同类型的窗口进行定义和处理的接口，而在 Table API 和 Sql 中，类似的功能也可以实现。

### 分组窗口(Group Window，老版本)

在 Flink 1.12 之前的版本中，Table API 和 Sql 提供了一组“分组窗口”（Group Window）函数，常用的时间窗口如滚动窗口，滑动窗口，会话窗口都有对应的实现；具体的 Sql 中就是调用 TUMBLE(),HOP()、SESSION(), 闯入时间属性字段，窗口大小参数就可以了。以滚动窗口为例：
```sql
TUMBLE(ts, INTERVAL '1' HOUR)
```
这里的 ts 是定义好的时间属性字段。窗口大小用 “时间间隔” INTERVAL 来定义。

在进行窗口计算时，分组窗口是将窗口本身当做一个字段进行分组的，可以对组内的数据进行聚合。基本使用方式如下：
```sql
Table result = tableEnv.sqlQuery(
 "SELECT " +
 "user, " +
"TUMBLE_END(ts, INTERVAL '1' HOUR) as endT, " +
 "COUNT(url) AS cnt " +
 "FROM EventTable " +
 "GROUP BY " + // 使用窗口和用户名进行分组
 "user, " +
 "TUMBLE(ts, INTERVAL '1' HOUR)" // 定义 1 小时滚动窗口
 );
```
这里定义了 1 小时的滚动窗口，将窗口和用户 user 一起作为分组的字段。用聚合函数 count() 对分组数据的个数进行了聚合统计，并将结果字段重命名为 cnt; 使用 TUMBLE_END() 函数将获取滚动窗口的结束时间，重名为为 endT 给提取出来。分组窗口的功能比较有限，只支持聚合，所以目前已经处于启用(deprecated) 的状态。

### 窗口表值函数(Windowing TVFs，新版本)

从 1.13 版本开始，Flink 开始使用窗口表值函数(Windowing table-valued functions, Windowing TVFs)来定义窗口。窗口表值函数是 Flink 定义的多态表函数(PTF),可以将表扩展后进行返回。

目前 Flink 提供了一下几个窗口 TVF ：

* 滚动窗口(Tumbling Windows)
* 滑动窗口(Hop Windows,跳跃窗口)
* 累计窗口(Cumulate Windows)
* 会话窗口(Session Windows，目前尚未完全支持)

窗口表值函数可以完全替代传统的分组窗口函数。窗口 TVF 更符合 SQL 标准，性能得到了优化，拥有更强大的功能；可以支持基于窗口的复杂运算，比如窗口 Top-N, 窗口连接(window join) 等等。当然，当前窗口 TVF 的功能还不完善，会话窗口和很多高级功能还不支持，不过正在快速地更新完善。可以预见在未来的版本中，窗口 TVF 将越来越强大，将会是窗口处理的唯一入口。

在窗口 TVF 的返回值中，出去原始表中的所有列，还增加了用来描述窗口的额外 3 个列：“窗口起始点”(window_start), 「窗口结束点」(window_end),窗口时间(window_time)。起始点和结束点比较好理解，这里的窗口时间指的是窗口中的时间属性，它的值等于 window_end-1ms，所以相当于是窗口中能包含数据的最大时间戳。

在 Sql 中的声明方式，与以往的分组窗口是类似的，直接调用 TUMBLE(),HOP(),CUMULATE() 就可以实现滚动、滑动和累计窗口，不过传入的参数会有所不同。

#### 滚动窗口(TUMBLE)

滚动窗口在 SQL 中的概念与 DataStream API 中定义的完全一样，是长度固定、时间对齐、无重叠的窗口，一般用于周期性的统计计算。

在Sql 中通过调用 TUMBLE() 函数就可以声明一个滚动窗口，只有一个核心参数就是窗口大小(size)。在 SQL 中不考虑计数窗口(数据滚动-DataStream API 中数据流中多少条数据组成一个窗口)，所以滚动窗口就是滚动的时间窗口，参数中还需要将当前的时间属性字段传入；另外，窗口 TVF 本质上是表函数，可以对表进行扩展，所以还应该当前查询的表作为整体参数传入。具体声明如下：
```sql
TUMBLE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR)
```

这里基于时间字段 ts，对表 EventTable 中的数据开了大小为 1 个小时的滚动窗口。窗口会将表中的每一行数据，按照他们 ts 的值分配到一个指定的窗口函数中。

#### 滑动窗口(HOP)

滑动窗口的使用与滚动窗口类似，可以通过设置滑动步长来控制统计输出的频繁。在 Sql 中通过调用 HOP() 来声明滑动窗口；除了也要传入表名，时间属性外，还需要传入大小(size)和滑动步长(slide) 两个参数。

```sql
HOP(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '5' MINUTES, INTERVAL '1' HOURS));
```
这里我们基于时间属性 ts，在表 EventTable 上创建了大小为 1 小时的滑动窗口，每 5 分钟滑动一次。需要注意的是，紧跟在时间属性字段的第 3 个字段才是步长(slide)，第 4 个参数才是窗口大小(size)。

#### 累计窗口(CUMLATE)

滚动窗口和滑动窗口，可以用来计算大多数周期性统计指标。不过在实际应用中还会遇到这样一类需求：我们的统计周期可能比较长，因此希望中间每隔一段时间就输出一次当前的统计值；与滑动窗口不同的是，在同一个统计周期内，我们会多次输出统计值，它们应该是不断叠加累积的。

例如，我们按天来统计网站的 PV，如果用 1 天的滚动窗口，那需要到每天的 24 点才会计算一次，输出频率太低；如果用滑动窗口，计算频率可以更高，但统计的就变成了“过去 24 小时的 PV”。所以我们真正希望的是，还是按照自然日统计每天的 PV，不过需要每隔 1 小时就输出一次当天到目前为止的 PV 值。这种特殊的窗口就叫做“累计窗口”(Cumulate Window)。

累计窗口是窗口 TVF 中新增的窗口功能，它会在一定的统计周期内进行累计计算。累计窗口中有两个核心的参数，最大窗口长度(max window size) 和 累计步长(step)。所谓的最大窗口长度其实就是我们所说的“统计周期”，最终目的就是统计这段时间内的数据。开始是，创建的第一个窗口大小就是步长 step; 之后的每个窗口都会之前的基础上再扩展 step 的长度，直到达到最大窗口长度。在 Sql 中可以用 cumulate() 函数来定义，具体如下：

```sql
CUMULATE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOURS, INTERVAL '1' DAYS))
```

这里我们基于时间属性 ts, 在表 EventTable 上定义了一个统计周期为 1 天，累计步长为 1 小时的累计窗口，注意第 3 个参数为步长 step，第 4 个参数为最大窗口长度。

上面的语句只是定义了窗口，类似于 DataStream API 中的窗口分配器；在Sql 中窗口的完整调用，还需要配合聚合操作和其他操作。


