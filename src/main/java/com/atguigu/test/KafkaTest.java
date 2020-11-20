package com.atguigu.test;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Collections;
import java.util.Properties;

/**
 * @author TangZC
 * @create 2020-11-18 9:04
 */
public class KafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("test",
                new SimpleStringSchema(),
                properties
        ));

        SingleOutputStreamOperator<SensorReading> sensorDS = kafkaDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0],
                        Long.parseLong(split[1]),
                        Double.parseDouble(split[2]));
            }
        });

        SplitStream<SensorReading> split = sensorDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ?
                        Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");

        high.print("high");
        low.print("low");

        env.execute();

    }
}
