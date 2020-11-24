package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.omg.CORBA.INTERNAL;

/**
 * @author TangZC
 * @create 2020-11-24 20:09
 */
public class FlinkSQL11_Function_AggFunc {

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

        tableEnv.registerFunction("TempAvg", new TempAvg());

        Table tableResult = table.groupBy("id").select("id, temp.TempAvg");

        tableEnv.createTemporaryView("sensor", table);
        Table sqlResult = tableEnv.sqlQuery("select id, TempAvg(temp) from sensor group by id");

        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sqlResult");

        env.execute();
    }

    public static class TempAvg extends AggregateFunction<Double, Tuple2<Double, Integer>>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0D, 0);
        }

        //计算方法
        public void accumulate(Tuple2<Double, Integer> buffer, Double value) {
            buffer.f0 += value;
            buffer.f1 += 1;
        }

        @Override
        public Double getValue(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
        }
    }
}
