package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author TangZC
 * @create 2020-11-23 20:02
 */
public class FlinkSQL09_Sink_ES_Upsert {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        Table table = tableEnv.fromDataStream(sensorDS);

        table.groupBy("id").select("id, id.count as ct");
        tableEnv.createTemporaryView("socket", table);
        Table sqlResult = tableEnv.sqlQuery("select id, ts, count(id) as ct from socket group by id, ts");

        tableEnv.connect(new Elasticsearch()
            .version("6")
            .host("hadoop102", 9200, "http")
            .index("flink_sql04")
            .disableFlushOnCheckpoint()
            .bulkFlushMaxActions(1)
            .documentType("_doc"))
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts", DataTypes.BIGINT())
                    .field("ct", DataTypes.BIGINT()))
                .createTemporaryTable("EsPath");

        tableEnv.insertInto("EsPath", sqlResult);

        tableEnv.toRetractStream(sqlResult, Row.class).print();

        env.execute();
    }
}
