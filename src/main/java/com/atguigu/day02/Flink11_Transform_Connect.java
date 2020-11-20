package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author TangZC
 * @create 2020-11-17 18:12
 */
public class Flink11_Transform_Connect {
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
        SplitStream<SensorReading> split = sensorDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ?
                        Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Double>> high = split.select("high").map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, Double>(sensorReading.getId(), sensorReading.getTemp());
            }
        });
        DataStream<SensorReading> low = split.select("low");

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = high.connect(low);

        SingleOutputStreamOperator<Object> map = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<String, Double, String>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "warn");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, String>(sensorReading.getId(), "healthy");
            }
        });

        map.print();
        env.execute();
    }
}
