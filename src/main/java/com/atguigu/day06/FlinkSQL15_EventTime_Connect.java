package com.atguigu.day06;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author TangZC
 * @create 2020-11-23 21:10
 */
public class FlinkSQL15_EventTime_Connect {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new FileSystem().path("sensor"))
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts", DataTypes.BIGINT())
                    .field("temp", DataTypes.DOUBLE())
                    .field("rt", DataTypes.BIGINT()).rowtime(new Rowtime()
                        .timestampsFromField("ts")
                        .watermarksPeriodicBounded(1000))
                ).withFormat(new Csv()).createTemporaryTable("fileInput");

        Table table = tableEnv.from("fileInput");
        table.printSchema();
    }
}
