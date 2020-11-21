package com.atguigu.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author TangZC
 * @create 2020-11-21 8:43
 */
public class Test04 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String, Integer>(split[0], 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = map.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> winDS = keyDS.timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut"){});

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = winDS.sum(1);

        sum.print("main");

        DataStream<Tuple2<String, Integer>> sideOutPut = sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutPut"){});

       sideOutPut.print("sideOutPut");

        env.execute();
    }
}
