package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author TangZC
 * @create 2020-11-24 20:00
 */
public class FlinkSQL10_Function_TableFunc {

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

        Table table = tableEnv.fromDataStream(map);

        tableEnv.registerFunction("Split", new Split());

        Table tableResult = table.joinLateral("Split(id) as (word, length)").select("id, word, length");

        tableEnv.createTemporaryView("sensor", table);
        Table sqlResult = tableEnv.sqlQuery("select id, word, length from sensor,LATERAL TABLE(Split(id)) as T(word, length)");

        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        env.execute();
    }

    public static class Split extends TableFunction<Tuple2<String, Integer>> {

        public void eval(String value) {
            String[] split = value.split("_");
            for (String s : split) {
                collector.collect(new Tuple2<>(s, s.length()));
            }
        }
    }
}
