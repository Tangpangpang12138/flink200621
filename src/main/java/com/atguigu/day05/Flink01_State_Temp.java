package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author TangZC
 * @create 2020-11-21 11:25
 */
public class Flink01_State_Temp {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取端口数据创建流
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> keyDS = map.keyBy("id");

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> result = keyDS.flatMap(new MyTempIncFunc());

        result.print();

        env.execute();
    }

    public static class MyTempIncFunc extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        //声明上一次温度值的范围
        private ValueState<Double> lastTemp = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            //给状态做初始化
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            //获取上一次温度值以及当前的温度值
            Double lastTempValue = lastTemp.value();
            Double curTemp = sensorReading.getTemp();

            //判断跳变量是否超过30度
            if(lastTempValue != null && Math.abs(lastTempValue - curTemp) > 10.0) {
                collector.collect(new Tuple3<String, Double, Double>(sensorReading.getId(), lastTempValue, curTemp));
            }

            //更新状态
            lastTemp.update(curTemp);
        }
    }
}
