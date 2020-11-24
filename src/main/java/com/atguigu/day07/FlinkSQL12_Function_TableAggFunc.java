package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


/**
 * @author TangZC
 * @create 2020-11-24 20:26
 */
public class FlinkSQL12_Function_TableAggFunc {

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

        tableEnv.registerFunction("Top2Temp", new Top2Temp());

        Table tableResult = table.groupBy("id").flatAggregate("Top2Temp(temp) as (temp, rank)").select("id, temp, rank");

        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");

        env.execute();

    }

    public static class Top2Temp extends TableAggregateFunction<Tuple2<Double, Integer>, Tuple2<Double, Double>> {

        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<>(Double.MIN_VALUE, Double.MIN_VALUE);
        }

        public void accumulate(Tuple2<Double, Double> buffer, Double value) {
            //将输入数据跟第一个比较
            if(value > buffer.f0){
                buffer.f1 = buffer.f0;
                buffer.f0 = value;
            } else if (value > buffer.f1) {
                //将输入数据跟第二个比较
                buffer.f1 = value;
            }
        }

        public void emitValue(Tuple2<Double, Double> buffer, Collector<Tuple2<Double, Integer>> collector) {

            collector.collect(new Tuple2<>(buffer.f0, 1));
            if(buffer.f1 != Double.MIN_VALUE) {
                collector.collect(new Tuple2<>(buffer.f1, 2));
            }
        }
    }
}
