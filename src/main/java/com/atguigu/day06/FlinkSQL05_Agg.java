package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author TangZC
 * @create 2020-11-23 18:56
 */
public class FlinkSQL05_Agg {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> input = env.readTextFile("sensor");

        SingleOutputStreamOperator<SensorReading> sensorDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryView("sensor", sensorDS);

        Table table = tableEnv.from("sensor");
        Table tableResult = table.groupBy("id").select("id, id.count, temp.avg");

        Table sqlResult = tableEnv.sqlQuery("select id, count(id) from sensor group by id");

        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sqlResult");

        env.execute();

    }

}
