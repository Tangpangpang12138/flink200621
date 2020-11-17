package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.PriorityBlockingQueue;

/**
 * @author TangZC
 * @create 2020-11-16 14:49
 */
public class Flink_WordCount_Bounded {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineDS = env.readTextFile("input");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new Flink01_Wordcount_Batch.MyFlatMap());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordToOneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.sum(1);

        result.print();

        env.execute("Flink_WordCount_Bounded");

    }
}
