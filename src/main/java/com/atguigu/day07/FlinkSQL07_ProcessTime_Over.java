package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author TangZC
 * @create 2020-11-24 19:16
 */
public class FlinkSQL07_ProcessTime_Over {

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

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(map, "id,ts,temp,pt.proctime");

//        Table result = table.window(Over.partitionBy("id").orderBy("pt").as("ow")).select("id, id.count over ow");

//        Table result = table.window(Over.partitionBy("id").orderBy("pt").preceding("3.rows").as("ow")).select("id, id.count over ow");

        tableEnv.createTemporaryView("sensor", table);
        Table result = tableEnv.sqlQuery("select id, count(id) over(partition by id order by pt) as ct from sensor");

        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}
