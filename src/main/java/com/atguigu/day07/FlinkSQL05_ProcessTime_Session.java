package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author TangZC
 * @create 2020-11-24 18:53
 */
public class FlinkSQL05_ProcessTime_Session {

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

        //4.会话窗口
//        Table result = table.window(Session.withGap("5.seconds").on("pt").as("sw"))
//                .groupBy("id,sw")
//                .select("id,id.count");

        //SQL
        tableEnv.createTemporaryView("sensor", table);
        Table result = tableEnv.sqlQuery("select id,count(id) as ct from sensor " +
                "group by id,session(pt,INTERVAL '5' second)");

        //5.转换为流进行输出
        tableEnv.toAppendStream(result, Row.class).print();

        //6.执行
        env.execute();
    }
}
