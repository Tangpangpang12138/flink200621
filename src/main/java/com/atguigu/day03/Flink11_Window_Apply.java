package com.atguigu.day03;

import com.atguigu.day01.Flink01_Wount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author TangZC
 * @create 2020-11-18 20:58
 */
public class Flink11_Window_Apply {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new Flink01_Wount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordToOne.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<Integer> apply = window.apply(new MyWindowFunc());

        apply.print();

        env.execute();
    }

    public static  class MyWindowFunc implements WindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow> {


        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Integer> collector) throws Exception {
            Integer count = 0;

            Iterator<Tuple2<String, Integer>> iterator = iterable.iterator();

            while (iterator.hasNext()) {
                Tuple2<String, Integer> next = iterator.next();
                count += 1;
            }

            collector.collect(count);
        }
    }
}
