package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;


/**
 * @author TangZC
 * @create 2020-11-23 18:32
 */
public class FlinkSQL04_Source_Kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建kafka连接器
        tableEnv.connect(new Kafka()
            .topic("test")
            .version("0.11")
            .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
            .property(ConsumerConfig.GROUP_ID_CONFIG, "testKafkaSource"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaInput");

        //创建表
        Table table = tableEnv.from("kafkaInput");

        //TableAPI
        Table tableResult = table.where("id = sensor_1").select("id, temp");

        //SQL
        Table sqlResult = tableEnv.sqlQuery("select id, temp from kafkaInput where id = 'sensor_1'");


        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");//
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        env.execute();

    }
}
