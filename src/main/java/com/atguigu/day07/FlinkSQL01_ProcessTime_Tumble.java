package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author TangZC
 * @create 2020-11-24 15:42
 */
public class FlinkSQL01_ProcessTime_Tumble {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        Table table = tableEnv.fromDataStream(map, "id, ts, temp, pt.proctime");

//        Table result = table.window(Tumble.over("5.seconds").on("pt").as("sw"))
//                .groupBy("id, sw")
//                .select("id, id.count");


//        Table result = table.window(Slide.over("5.rows").every("2.rows").on("pt").as("sw"))
//                .groupBy("id, sw")
//                .select("id, id.count");

        tableEnv.createTemporaryView("sensor", table);
        Table result = tableEnv.sqlQuery("select id, count(id) as ct, TUMBLE_end(pt,INTERVAL '5' second) from sensor group by id, TUMBLE(pt,INTERVAL '5' second)");

        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
