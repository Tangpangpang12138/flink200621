package com.atguigu.test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author TangZC
 * @create 2020-11-18 9:24
 */
public class Test02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fileDs = env.readTextFile("input");

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = fileDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(new Tuple2<String, Integer>(s2, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = flatMap.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyBy.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<String, Integer>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> filter = reduce.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                int value = stringIntegerTuple2.f1;
                return value == 1;
            }
        });

//        SingleOutputStreamOperator<String> map = filter.map(new MapFunction<Tuple2<String, Integer>, String>() {
//            @Override
//            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                String x = stringIntegerTuple2.f0;
//                return x;
//            }
//        });

        filter.print();
        env.execute();


    }
}
