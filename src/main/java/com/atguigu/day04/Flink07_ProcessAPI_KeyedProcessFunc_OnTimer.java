package com.atguigu.day04;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author TangZC
 * @create 2020-11-20 20:18
 */
public class Flink07_ProcessAPI_KeyedProcessFunc_OnTimer {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //3.将每一行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //4.分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        //使用ProcessAPI处理数据
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyKeyedProcessFunc());

        result.print();

        env.execute();

    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, String> {

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {

            //定时服务相关
            TimerService timerService = context.timerService();
            long ts = timerService.currentProcessingTime();

            System.out.println(ts);
            timerService.registerProcessingTimeTimer(ts + 5000L);

            collector.collect(sensorReading.getId());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("Time" + timestamp);
            out.collect("定时器工作了！");
        }
    }
}
