package com.atguigu.day04;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author TangZC
 * @create 2020-11-20 20:38
 */
public class Flink08_ProcessAPI_KeyedProcessFunc_SideOutPut {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //3.将每一行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //4.分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        //5.使用ProcessAPI数据
        SingleOutputStreamOperator<SensorReading> highDS = keyedStream.process(new MyKeyedProcessFunc());

        DataStream<String> lowDS = highDS.getSideOutput(new OutputTag<String>("low") {
        });

        highDS.print("high");
        lowDS.print("low");

        env.execute();

    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, SensorReading> {

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {

            //获取数据中的温度值
            Double temp = sensorReading.getTemp();

            //根据温度高低将数据发送到不同的流
            if(temp > 30.0D) {
                //高温数据,发送数据至主流
                collector.collect(sensorReading);
            } else {
                //低温数据,发送数据至低温流
                context.output(new OutputTag<String>("low"){}, sensorReading.getId());
            }
        }
    }
}
