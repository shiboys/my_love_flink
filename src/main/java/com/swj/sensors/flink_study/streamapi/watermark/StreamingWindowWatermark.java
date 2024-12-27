package com.swj.sensors.flink_study.streamapi.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/29 20:48
 * WaterMark 使用方法展示
 */
public class StreamingWindowWatermark {

  private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
  static final OutputTag<Tuple2<String, Long>> outputLateTag = new OutputTag<Tuple2<String, Long>>("out-of-date") {
  };
  static final String defaultTimestampFormat = "[%d|%s]";
  static final String watermarkMsgFormat = "key:%s, eventTime:%s, currentMaxTimestamp:%s, watermark:%s";


  public static void main(String[] args) throws Exception {
    int port = 9000;
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", port);
    SingleOutputStreamOperator<Tuple2<String, Long>> mapSource =
        socketTextStream.map(new MapFunction<String, Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Long> map(String value) throws Exception {
            String[] arr = value.split(",");
            return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
          }
        });
    // mapSource 生成完之后，对其进行 watermark 生成设置 生成一个 watermarkStream, 然后用该 stream 设置 keyBy,
    // window, 和 applyWindowFunction, 最后将窗口数据和 outofdate  的 sideout 数据写出
    SingleOutputStreamOperator<Tuple2<String, Long>> watermarkStream =
        mapSource.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

          private long maxTimestamp;

          @Override
          public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.f1);
            System.out.println(String.format(watermarkMsgFormat, element.f0, formattedTimestamp(element.f1),
                formattedTimestamp(maxTimestamp), formattedTimestamp(getCurrentWatermark().getTimestamp())));
            return maxTimestamp;
          }

          @Nullable
          @Override
          public Watermark getCurrentWatermark() {
            // watermark 的生成时间默认比 eventTime 晚 10分钟
            long defaultLateness = 10_000;
            return new Watermark(maxTimestamp - defaultLateness);
          }
        });

    SingleOutputStreamOperator<String> resultStream = watermarkStream.keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.seconds(3)))
        .sideOutputLateData(outputLateTag)
        .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
          @Override
          public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out)
              throws Exception {
            String key = tuple.toString();
            Iterator<Tuple2<String, Long>> it = input.iterator();
            List<Long> tsList = new ArrayList<>();
            while (it.hasNext()) {
              tsList.add(it.next().f1);
            }
            // 将当前窗口中的所有 timestamp 进行排序,
            Collections.sort(tsList);
            String msg = "key=" + key + ",size=" + tsList.size() + ", start timestamp=" + SDF.format(tsList.get(0))
                + ", end timestamp=" + SDF.format(tsList.get(tsList.size() - 1))
                + ", window start ts=" + SDF.format(window.getStart()) + ", window end ts=" + SDF.format(
                window.getEnd());
            out.collect(msg);
          }
        });

    DataStream<Tuple2<String, Long>> sideOutput = resultStream.getSideOutput(outputLateTag);
    // emit the result

    if(parameterTool.has("output") && parameterTool.has("lateness-output")) {
      sideOutput.writeAsText(parameterTool.get("lateness-output"));
      resultStream.writeAsText(parameterTool.get("output"));
    } else {
      // 把结果打印在控制台
      sideOutput.print();
      resultStream.print();
    }
    //因为 flink 是懒加载的，必须调用 execute，上面的方法才会被执行
    env.execute("Watermark Generate Example");
  }

  static String formattedTimestamp(long timestamp) {
    if (timestamp < 1) {
      return "";
    }
    return String.format(defaultTimestampFormat, timestamp, SDF.format(timestamp));
  }
}
