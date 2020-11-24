package com.atguigu.day06;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author TangZC
 * @create 2020-11-23 21:16
 */
public class FlinkSQL16_EventTime_DDL {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        String sinkDDL = "create table dataTable (" +
                " id varchar(20) not null, " +
                " ts bigint, " +
                " temp double, " +
                " rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ), " +
                " watermark for rt as rt - interval '1' second" +
                ") with (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'sensor', " +
                " 'format.type' = 'csv')";

        tableEnv.sqlUpdate(sinkDDL);

        Table table = tableEnv.from("dataTable");
        table.printSchema();
    }
}
