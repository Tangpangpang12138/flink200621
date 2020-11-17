package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author TangZC
 * @create 2020-11-17 14:49
 */
public class Flink05_Transform_Map {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        SingleOutputStreamOperator<SensorReading> sensor = sensorDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) {
                String[] fields = value.split(",");
                return new SensorReading(fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2])
                );
            }
        });


        sensor.print();

        env.execute();

    }
}
