package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author TangZC
 * @create 2020-11-23 20:58
 */
public class FlinkSQL13_ProcessTime_DDL {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings bsSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSetting);

        String sinkDDL = "create table dataTable (" +
                " id varchar(20) not null, " +
                " ts bigint, " +
                " temp double, " +
                " pt AS PROCTIME() " +
                ") with (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'sensor', " +
                " 'format.type' = 'csv')";

        bsTableEnv.sqlUpdate(sinkDDL);

        Table table = bsTableEnv.from("dataTable");
        table.printSchema();
    }
}
