package com.swj.sensors.flink_study.streamapi.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/01 11:22
 */
public class StreamingWithRedisSinkExample {
  public static void main(String[] args) throws Exception {
    /**
     * 从网络中读取内容，然后 sink 到 redis 中
     * 采用的 是 flink 的 redis connect
     * redis 的命令时 lpush
     */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9000, "\n");

    // lpush l_words word
    // 先将 String 转换为 tuple<String,String> 的 <key,value> 形式
    final String redisKey = "l_words";
    DataStream<Tuple2<String, String>> wordsSource =
        socketTextStream.map(new MapFunction<String, Tuple2<String, String>>() {
          @Override
          public Tuple2<String, String> map(String value) throws Exception {
            return new Tuple2<>(redisKey, value);
          }
        });

    FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
        .setHost("localhost")
        .setPort(6379)
        .build();

    RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(redisConfig, new MyRedisMapper());

    wordsSource.addSink(redisSink);

    env.execute("StreamingWithRedisSinkExample");
    /**
     * 运行结果如下
     * 127.0.0.1:6379> lrange l_words 0 10
     *  1) "I,1701268072176"
     *  2) "M,1701268065904"
     *  3) "S,1701268104017"
     *  4) "H,1701268096293"
     *  5) "U,1701268089691"
     *  6) "M,1701268104971"
     *  7) "K,1701268100957"
     *  8) "I,1701268067530"
     *  9) "R,1701268106781"
     * 10) "V,1701268098944"
     * 11) "N,1701268098461"
     */
  }

  static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

    @Override
    public RedisCommandDescription getCommandDescription() {
      return new RedisCommandDescription(RedisCommand.LPUSH);
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
      return stringStringTuple2.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
      return stringStringTuple2.f1;
    }
  }
}
