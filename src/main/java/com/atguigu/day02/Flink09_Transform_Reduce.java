package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author TangZC
 * @create 2020-11-17 16:57
 */
public class Flink09_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fileDS = env.readTextFile("sensor");

        SingleOutputStreamOperator<SensorReading> sensorDS = fileDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0],
                        Long.parseLong(split[1]),
                        Double.parseDouble(split[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        SingleOutputStreamOperator<SensorReading> reduce = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return new SensorReading(sensorReading.getId(),
                        t1.getTs(),
                        Math.max(sensorReading.getTemp(), t1.getTemp()));
            }
        });

        reduce.print();

        env.execute();
    }
}
