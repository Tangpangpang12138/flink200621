package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author TangZC
 * @create 2020-11-17 19:21
 */
public class Flink06_Transform_Flatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fileDS = env.readTextFile("sensor");

        SingleOutputStreamOperator<String> flatDS = fileDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        flatDS.print();
        env.execute();

    }
}
