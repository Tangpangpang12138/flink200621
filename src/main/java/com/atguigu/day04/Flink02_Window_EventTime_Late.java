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
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author TangZC
 * @create 2020-11-20 18:06
 */
public class Flink02_Window_EventTime_Late {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String, Integer>(split[0], 1);
            }
        });

        //重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = wordToOne.keyBy(0);

        //开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> winDS = keyDS.timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
                });

        //计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = winDS.sum(1);

        sum.print("main");

        //获取侧输出流数据
        DataStream<Tuple2<String, Integer>> sideOutput = sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {});

        sideOutput.print("sideOutPut");

        //执行任务
        env.execute();

    }
}
