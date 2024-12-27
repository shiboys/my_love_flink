/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.swj.sensors.flink_study;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class SocketTextStreamingWordCount {

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String hostName = parameterTool.get("host");
    String portStr = parameterTool.get("port");

    if (hostName == null || portStr == null) {
      System.err.println("USAGE:\nSocketTextStreamingWordCount --host <hostname> --port <port>");
      return;
    }
    int port = Integer.parseInt(portStr);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<String> socketSourceStream = env.socketTextStream(hostName, port);
    SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketSourceStream.flatMap(new LineSplitter())
        .keyBy(0)
        .sum(1);
    sum.print();
    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     * 	env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream<String> using operations
     * like
     * 	.filter()
     * 	.flatMap()
     * 	.join()
     * 	.coGroup()
     *
     * and many more.
     * Have a look at the programming guide for the Java API:
     *
     * https://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    // execute program
    env.execute("My First Flink Streaming Java API Test");
  }

  static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
      String[] splitArr = value.split("\\W+"); // 所有的 非字母数字下划线为分隔符
      for (String token : splitArr) {
        if (token.isEmpty()) {
          continue;
        }
        out.collect(new Tuple2<>(token, 1));
      }
    }
  }
}


