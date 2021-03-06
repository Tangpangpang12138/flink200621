package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.concurrent.ExecutionContextExecutor;

/**
 * @author TangZC
 * @create 2020-11-16 14:37
 */
public class Flink01_Wordcount_Batch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> lineDS = env.readTextFile("input");

        FlatMapOperator<String, Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new MyFlatMap());

        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = wordToOneDS.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> result = groupByDS.sum(1);

        result.print();

    }
    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>>{

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");

            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
