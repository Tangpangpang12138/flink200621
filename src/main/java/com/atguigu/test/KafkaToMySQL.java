package com.atguigu.test;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.elasticsearch.common.recycler.Recycler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author TangZC
 * @create 2020-11-20 8:38
 */
public class KafkaToMySQL {
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


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(new Tuple2<String, Integer>(s2, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = wordToOne.keyBy(0);


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);

        sum.print();

        sum.addSink(new MySQLSink());

        env.execute();



    }

    public static class MySQLSink extends RichSinkFunction<Tuple2<String, Integer>> {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            preparedStatement = connection.prepareStatement("INSERT INTO wordcount(word,shu) VALUES(?,?) ON DUPLICATE KEY UPDATE shu=?");
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            preparedStatement.setString(1, value.f0);
            preparedStatement.setInt(2, value.f1);
            preparedStatement.setInt(3, value.f1);

            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
