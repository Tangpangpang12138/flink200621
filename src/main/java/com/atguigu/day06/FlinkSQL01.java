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
 * @create 2020-11-23 10:15
 */
public class FlinkSQL01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取文本数据创建流并转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.readTextFile("sensor").map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

//        DataStreamSource<String> readTextFile = env.readTextFile("sensor");
//
//        //3.将每一行数据转换为JavaBean
//        SingleOutputStreamOperator<SensorReading> sensorDS = readTextFile.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] split = value.split(",");
//                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
//            }
//        });

        //创建TableAPI FlinkSQL的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //从流中创建表
        Table table = tableEnv.fromDataStream(sensorDS);

        //转换数据
        //使用TableAPI转换数据
        Table result = table.select("id,temp").filter("id = 'sensor_1'");

        //使用FlinkSQL转换数据
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor where id='sensor_1'");

        //转换为流输出数据
        tableEnv.toAppendStream(result,Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sql");

        env.execute();

    }
}
