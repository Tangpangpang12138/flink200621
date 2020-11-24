package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author TangZC
 * @create 2020-11-23 20:35
 */
public class FlinkSQL12_ProcessTime_Connect {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new FileSystem().path("sensor"))
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts", DataTypes.BIGINT())
                    .field("temp", DataTypes.DOUBLE())
                    .field("pt", DataTypes.TIMESTAMP(3)).proctime())
                    .withFormat(new OldCsv())
                    .createTemporaryTable("fileInput");

        Table table = tableEnv.from("fileInput");
        table.printSchema();
    }
}
