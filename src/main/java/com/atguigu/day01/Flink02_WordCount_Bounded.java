package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author TangZC
 * @create 2020-11-16 13:52
 */
public class Flink02_WordCount_Bounded {

    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文本数据
        DataStreamSource<String> lineDataStream = env.readTextFile("input");

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToDataStream = lineDataStream.flatMap(new Flink01_Wount_Batch.MyFlatMapFunc());

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordToDataStream.keyBy(0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.sum(1);

        //打印
        result.print();

        //开启任务
        env.execute("Flink02_WordCount_Bounded");
    }
}
