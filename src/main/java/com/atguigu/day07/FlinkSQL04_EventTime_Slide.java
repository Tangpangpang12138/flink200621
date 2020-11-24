package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author TangZC
 * @create 2020-11-24 18:45
 */
public class FlinkSQL04_EventTime_Slide {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(String element) {
                        String[] split = element.split(",");
                        return Long.parseLong(split[1]) * 1000L;
                    }
                });

        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        Table table = tableEnv.fromDataStream(map, "id, ts, temp, rt.rowtime");

        Table result = table.window(Slide.over("6.seconds").every("2.seconds").on("rt").as("tw")).groupBy("id, tw").select("id, id.count, tw.end");

        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
