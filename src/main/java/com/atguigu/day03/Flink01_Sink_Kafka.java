package com.atguigu.day03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author TangZC
 * @create 2020-11-18 13:41
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("sensor");

        inputDS.addSink(new FlinkKafkaProducer011<String>("hadoop102:9092","test",new SimpleStringSchema()));

        env.execute();
    }
}
