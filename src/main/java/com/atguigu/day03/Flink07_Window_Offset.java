package com.atguigu.day03;

import com.atguigu.day01.Flink01_Wount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author TangZC
 * @create 2020-11-18 20:01
 */
public class Flink07_Window_Offset {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new Flink01_Wount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordToOne.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)));

        window.sum(1).print();

        env.execute();
    }
}
