package com.swj.sensors.flink_study.streamapi.window_related;

import com.swj.sensors.flink_study.streamapi.wordcount.WordCount;
import com.swj.sensors.flink_study.streamapi.wordcount.util.WordCountData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/27 21:10
 */
public class WindowWordCount {
  public static void main(String[] args) throws Exception {
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    String inputFile = parameterTool.get("input");
    String outputFile = parameterTool.get("output");
    DataStream<String> source;
    if(parameterTool.has("input")) {
      source = executionEnvironment.readTextFile(inputFile);
    } else {
      System.out.println("Executing WindowWordCount with default data set.");
      System.out.println("Use -input to specify the file input.");
      source = executionEnvironment.fromElements(WordCountData.WORDS);
    }

    // make parameters available in the web interface
    executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);

    final int windowSize = parameterTool.getInt("window", 10 );
    final int slideSize = parameterTool.getInt("slide", 5);

    DataStream<Tuple2<String, Integer>> counts = source
        // split one line to one pair :tuple2 contains (word:1)
        .flatMap(new WordCount.Tokenizer())
        .keyBy(0)
        .countWindow(windowSize, slideSize)
        // group by filed 0 and sum up by field 1
        .sum(1);

    // emit result
    if(parameterTool.has("output")) {
       counts.writeAsText(outputFile);
    } else {
      System.out.println("Print result to stdout. use --output to specify the output file path.");
      counts.print();
    }
    /**
     * 输入结果如下：如果 window：10，slide：5 ，则如下：
     * (to,5)
     * (the,5)
     * (to,10)
     * (sleep,5)
     * (and,5)
     * (of,5)
     * (the,10)
     * (s,5)
     * (that,5)
     * (the,10)
     * (a,5)
     * (to,10)
     * (of,10)
     * (the,10)
     * (and,10)
     * (of,10) // of 总共 15 个，所以会跨 3 次窗口
     * 如果 window：2，slide：1 则如下：
     * (them,1)
     * (to,2)
     * (die,1)
     * (to,2)
     * (sleep,1)
     * (no,1)
     * (more,1)
     * (and,2)
     * (by,2)
     * (a,2)
     * (sleep,2)
     * (to,2)
     * (say,1)
     * (we,1)
     * (end,2)
     * 这说明 窗口每滑动一次，有满足的个数就触发，并将满足个数要求的窗口元素进行下发
     */

    executionEnvironment.execute("WindowWordCount");
  }
}
