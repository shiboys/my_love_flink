package com.swj.sensors.flink_study.streamapi.pu_uv_computing;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/02 15:26
 * Flink 处理 PV 和 UV 的主类
 */
public class PvAndUvExample {

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String topic = parameterTool.get("topic", Constants.TOPIC);
    String host = parameterTool.get("host", "localhost");
    String brokerUrl = parameterTool.get("brokers", host + ":9092");

    String className = PvAndUvExample.class.getSimpleName();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    Properties properties = new Properties();
    properties.put("group.id", className);
    properties.put("bootstrap.servers", brokerUrl);
    FlinkKafkaConsumer011<UserBehaviorEvent> kafkaConsumer =
        new FlinkKafkaConsumer011<>(topic, new UserEventSchema(), properties);
    System.out.printf("begin to fetch data from kafka,topic:%s,broker url:%s\n", topic, brokerUrl);
    System.out.println();
    kafkaConsumer.setStartFromLatest();
    /**
     * 函数式接口的用途
     * 它们主要用在Lambda表达式和方法引用（实际上也可认为是Lambda表达式）上。
     * 这个接口里面只能有一个抽象方法
     * 如定义了一个函数式接口如下：
     *
     *     @FunctionalInterface
     *     interface GreetingService
     *     {
     *         void sayMessage(String message);
     *     }
     * 那么就可以使用Lambda表达式来表示该接口的一个实现(注：JAVA 8 之前一般是用匿名类实现的)：
     *
     * GreetingService greetService1 = message -> System.out.println("Hello " + message);
     * forGenerator 方法的参数类型 WatermarkGeneratorSupplier 就是一个， 需要提供一个 watermarkGenerator ，
     * 而不用去匿名实现 WatermarkGeneratorSupplier 里面的具体的抽象方法
     */
    kafkaConsumer.assignTimestampsAndWatermarks(
        WatermarkStrategy.forGenerator((ctx) -> new PeriodicalWatermarkGenerator())
            .withTimestampAssigner((ctx) -> new TimestampExtractor())
    );

    DataStreamSource<UserBehaviorEvent> kafkaSource = env.addSource(kafkaConsumer);

    DataStream<Tuple4<Long, Long, Long, Integer>> uvCounter = kafkaSource
        .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
        // 允许 5 分钟的延迟时间
        .allowedLateness(Time.minutes(5))
        .process(new ProcessAllWindowFunction<UserBehaviorEvent, Tuple4<Long, Long, Long, Integer>, TimeWindow>() {
          @Override
          public void process(Context context, Iterable<UserBehaviorEvent> iterable,
              Collector<Tuple4<Long, Long, Long, Integer>> collector) throws Exception {
            long pv = 0;
            Iterator<UserBehaviorEvent> events = iterable.iterator();
            // uv 使用一个简单的 set 来计算 5 秒的窗口时间内的 uv 数
            Set<Integer> userIdSet = new HashSet<>();
            while (events.hasNext()) {
              UserBehaviorEvent event = events.next();
              pv++;
              userIdSet.add(event.getUserId());
            }
            TimeWindow window = context.window();
            collector.collect(new Tuple4<>(window.getStart(), window.getEnd(), pv, userIdSet.size()));
          }
        });

    uvCounter.print().setParallelism(1);

    // sink to elasticsearch
    String indexName = parameterTool.getRequired("index");
    uvCounter.addSink(buildElasticsearchSink(indexName));
    // sink to web socket
    String wsUrl = String.format(Constants.WS_URL, host);
    uvCounter.addSink(new WebSocketSink(wsUrl));
    env.execute(className);
  }

  static ElasticsearchSink<Tuple4<Long, Long, Long, Integer>> buildElasticsearchSink(String indexName) {

    HttpHost httpPost = new HttpHost("localhost", 9200, "http");

    ElasticsearchSink.Builder<Tuple4<Long, Long, Long, Integer>> esBuilder = new ElasticsearchSink.Builder<>(
        Collections.singletonList(httpPost),
        new ElasticsearchSinkFunction<Tuple4<Long, Long, Long, Integer>>() {
          @Override
          public void process(Tuple4<Long, Long, Long, Integer> tuple4, RuntimeContext runtimeContext,
              RequestIndexer requestIndexer) {
            requestIndexer.add(createIndexRequest(tuple4, indexName));
          }
        });
    esBuilder.setFailureHandler(new CustomFailureHandler(indexName));

    // this instructs the sink to emit after every element, otherwise they will be buffered
    esBuilder.setBulkFlushMaxActions(1);
    return esBuilder.build();
  }

  private static IndexRequest createIndexRequest(Tuple4<Long, Long, Long, Integer> tuple4, String indexName) {
    Map<String, Object> esIndexMapContent = new HashMap<>();
    esIndexMapContent.put("window_start", tuple4.f0);
    esIndexMapContent.put("window_end", tuple4.f1);
    esIndexMapContent.put("pv", tuple4.f2);
    esIndexMapContent.put("uv", tuple4.f3);

    // es 的 index 请求会用到 apache 的 httpClient 的 Patch 请求，因此必须引入 httpclient 包
    return Requests.indexRequest()
        .index(indexName)
        .id(tuple4.f1.toString())
        .source(esIndexMapContent);
  }

  private static class CustomFailureHandler implements ActionRequestFailureHandler {

    private static final long serialVersionUID = 942269087742453482L;
    private final String index;

    public CustomFailureHandler(String index) {
      this.index = index;
    }

    @Override
    public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer)
        throws Throwable {
      if (actionRequest instanceof IndexRequest) {
        IndexRequest oldRequest = (IndexRequest) actionRequest;
        Map<String, Object> newContentMap = new HashMap<>();
        newContentMap.put("data", oldRequest.source());
        IndexRequest newIndexRequest =
            Requests.indexRequest().index(this.index).source(newContentMap).id((oldRequest.id()));
        // 失败之后重新添加数据重新 发送数据到 ES
        requestIndexer.add(newIndexRequest);
      } else {
        System.err.println(throwable.getMessage());
        throw new IllegalStateException();
      }
    }
  }


  private static class PeriodicalWatermarkGenerator implements WatermarkGenerator<UserBehaviorEvent>, Serializable {

    private long currentTimestamp = Long.MIN_VALUE;

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者可以基于事件本身生成 watermark
     */
    @Override
    public void onEvent(UserBehaviorEvent userBehaviorEvent, long eventTimestamp, WatermarkOutput watermarkOutput) {
      currentTimestamp = Math.max(currentTimestamp, eventTimestamp);
    }

    /**
     * 周期性调用，也许会生成 watermark , 也许不会
     * 调用此方法生成 watermark 的间隔时间由 ExecutionConfig#getAutoWatermarkInterval() 决定
     * 周期性生成器通常通过 onEvent() 观察传入的事件数据，然后在框架调用 onPeriodicEmit() 时发出 watermark。
     * 标记生成器将查看 onEvent() 中的事件数据，并等待检查在流中携带 watermark 的特殊标记事件或打点数据。当获取到这些事件数据时，
     * 它将立即发出 watermark。通常情况下，标记生成器不会通过 onPeriodicEmit() 发出 watermark。
     *
     * @param watermarkOutput
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
      long availableWatermark = currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1;
      watermarkOutput.emitWatermark(new Watermark(availableWatermark));
    }
  }

  /**
   * 这个 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
   * 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
   */
  public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<UserBehaviorEvent> {

    private final long maxOutOfOrderness = 3500; // 3.5 秒

    private long currentMaxTimestamp;

    @Override
    public void onEvent(UserBehaviorEvent event, long eventTimestamp, WatermarkOutput output) {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }

  }

  /**
   * 该生成器生成的 watermark 滞后于处理时间固定量。它假定元素会在有限延迟后到达 Flink。
   */
  public class TimeLagWatermarkGenerator implements WatermarkGenerator<UserBehaviorEvent> {

    private final long maxTimeLag = 5000; // 5 秒

    @Override
    public void onEvent(UserBehaviorEvent event, long eventTimestamp, WatermarkOutput output) {
      // 处理时间场景下不需要实现
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      // 固定延迟。目测这个无法处理乱序的元素
      output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
  }


  /**
   * 自定义标记 Watermark 生成器
   * 标记 Watermark 生成器观察流事件数据并在获取到带有 watermark 信息的特殊事件元素时发出 watermark
   */
  public class PunctuatedAssigner implements WatermarkGenerator<MyEvent> {

    /**
     *
     * @param event
     * @param eventTimestamp
     * @param output
     */
    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
      if (event.isWatermarkMaker()) {
        output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
      }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      // onEvent 中已经实现
    }
    /**
     *  注意：可以针对每个事件去生成 watermark。但是由于每个 watermark 都会在下游做一些计算，因此过多的 watermark 会降低程序性能。
     */
  }


  private static class TimestampExtractor implements TimestampAssigner<UserBehaviorEvent> {

    @Override
    public long extractTimestamp(UserBehaviorEvent userBehaviorEvent, long l) {
      return userBehaviorEvent.getTs();
    }
  }

  @Getter
  @Setter
  static class MyEvent {
    boolean watermarkMaker;
    long watermarkTimestamp;
  }

}
