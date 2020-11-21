package com.atguigu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.security.Principal;

/**
 * @author TangZC
 * @create 2020-11-20 18:27
 */
public class Flink03_Window_EventTime_Watermark {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String, Integer>(split[0], 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = wordToOne.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> winDS = keyDS.timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = winDS.sum(1);

        sum.print();

        DataStream<Tuple2<String, Integer>> sideOutput = sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
        });
        sideOutput.print();

        env.execute();
    }
}
