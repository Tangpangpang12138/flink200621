package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author TangZC
 * @create 2020-11-21 11:43
 */
public class Flink02_State_OnTimer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> keyDS = map.keyBy("id");

        SingleOutputStreamOperator<String> result = keyDS.process(new MyKeyedProcessFunc());

        result.print();

        env.execute();
    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, String> {

        private ValueState<Double> lastTempState = null;
        private ValueState<Long> tsState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {

            Double lastTemp = lastTempState.value();
            Long lastTs = tsState.value();
            long ts = context.timerService().currentProcessingTime() + 5000L;

            //第一条数据,需要注册定时器
            if(lastTs == null) {
                context.timerService().registerProcessingTimeTimer(ts);
                tsState.update(ts);
            } else {
                if(lastTemp != null && sensorReading.getTemp() < lastTemp) {
                    context.timerService().deleteProcessingTimeTimer(tsState.value());
                    context.timerService().registerProcessingTimeTimer(ts);
                    tsState.update(ts);
                }
            }

            lastTempState.update(sensorReading.getTemp());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续10秒温度没有下降");
            tsState.clear();
        }
    }
}
