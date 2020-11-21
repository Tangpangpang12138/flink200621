//package com.atguigu.day04;
//
//
//import com.atguigu.bean.SensorReading;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
///**
// * @author TangZC
// * @create 2020-11-20 19:57
// */
//public class Flink06_ProcessAPI_KeyedProcessFunc {
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);
//
//        SingleOutputStreamOperator<SensorReading> sensorDS = input.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] split = s.split(",");
//                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
//            }
//        });
//
//        KeyedStream<SensorReading, Tuple> keyED = sensorDS.keyBy("id");
//
//        keyED.process(new MyKeyedProcessFunc())
//    }
//
//    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, String> {
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//        }
//
//        @Override
//        public void close() throws Exception {
//            super.close();
//        }
//
//        @Override
//        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
//
//
//        }
//    }
//}
