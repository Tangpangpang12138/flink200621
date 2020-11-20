package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author TangZC
 * @create 2020-11-18 14:32
 */
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("sensor");

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();
        inputDS.addSink(new RedisSink<String>(config, new MyRedisMapper()));

        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<String> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor");
        }

        @Override
        public String getKeyFromData(String s) {
            String[] split = s.split(",");
            return split[0];
        }

        @Override
        public String getValueFromData(String s) {
            String[] split = s.split(",");
            return split[2];
        }
    }
}
