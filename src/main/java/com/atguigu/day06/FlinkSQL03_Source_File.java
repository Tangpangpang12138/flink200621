package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author TangZC
 * @create 2020-11-23 18:23
 */
public class FlinkSQL03_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //定义文件连接器
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts", DataTypes.BIGINT())
                    .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("fileInput");

        //创建表
        Table table = tableEnv.from("fileInput");

        //TableAPI
        Table tableResult = table.where("id='sensor_1'").select("id,temp");

        //SQL
        Table sqlResult = tableEnv.sqlQuery("select id, temp from fileInput where id = 'sensor_1'");

        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        env.execute();
    }
}
