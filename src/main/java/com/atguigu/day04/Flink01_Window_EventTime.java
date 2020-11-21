package com.atguigu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import scala.collection.immutable.Stream;

/**
 * @author TangZC
 * @create 2020-11-20 17:44
 */
public class Flink01_Window_EventTime{
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定消费事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据,提取时间
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String, Integer>(split[0], 1);
            }
        });

        //重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = wordToOneDS.keyBy(0);

        //开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDS = keyDS.timeWindow(Time.seconds(5));

        //计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowDS.sum(1);

        //打印
        sum.print();

        //执行任务
        env.execute();
    }
}
