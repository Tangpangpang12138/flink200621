package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author TangZC
 * @create 2020-11-16 11:35
 */
public class Flink01_Wount_Batch {
    public static void main(String[] args) throws Exception {

        //1.创建Flink程序的入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取文件数据
        DataSource<String> lineDS = env.readTextFile("input");

        //压平
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new MyFlatMapFunc());

        //分组
        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = wordToOneDS.groupBy(0);

        //聚合计算
        AggregateOperator<Tuple2<String, Integer>> result = groupByDS.sum(1);

        //打印结果
        result.print();
    }

    //自定义的FlapMapFunction
    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String, Integer>>{

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

            //按照空格切分value
            String[] words = s.split(" ");

            //遍历words输出数据
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }

        }
    }
}
