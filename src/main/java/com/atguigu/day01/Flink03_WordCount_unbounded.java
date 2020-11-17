package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author TangZC
 * @create 2020-11-16 13:59
 */
public class Flink03_WordCount_unbounded {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //代码全局设置并行度
        env.setParallelism(2);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //从端口获取数据创建流
        DataStreamSource<String> lineDS = env.socketTextStream(host, port);

        //压平操作，给代码局部设置并行度
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new Flink01_Wount_Batch.MyFlatMapFunc()).setParallelism(3);

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        //计算聚合结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        //打印聚合结果
        result.print();

        //开启任务
        env.execute("Flink03_WordCount_unbounded");

    }
}
