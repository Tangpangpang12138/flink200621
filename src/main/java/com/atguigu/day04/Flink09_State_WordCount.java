package com.atguigu.day04;

import com.atguigu.day01.Flink01_Wount_Batch;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author TangZC
 * @create 2020-11-20 21:10
 */
public class Flink09_State_WordCount {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socketTextStream.flatMap(new Flink01_Wount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = flatMap.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> count = keyDS.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private ValueState<Integer> countState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class, 0));
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                //获取状态中的数据
                Integer value = countState.value();

                //累加
                value++;

                //更新状态
                countState.update(value);

                //最终返回数据
                return new Tuple2<String, Integer>(stringIntegerTuple2.f0, value);
            }
        });

        count.print();

        env.execute();
    }
}
