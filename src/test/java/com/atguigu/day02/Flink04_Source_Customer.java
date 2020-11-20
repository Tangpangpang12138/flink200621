package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Random;

/**
 * @author TangZC
 * @create 2020-11-17 14:36
 */
public class Flink04_Source_Customer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Object> mySource = env.addSource(new CustomerSource1());

        mySource.print();

        env.execute();

    }


    private static class CustomerSource1 implements org.apache.flink.streaming.api.functions.source.SourceFunction<Object> {

        private boolean running = true;

        Random random = new Random();

        @Override
        public void run(SourceContext<Object> sourceContext) throws Exception {
            HashMap<String, Double> tempMap = new HashMap<String, Double>();

            for (int i = 0; i < 10; i++) {
                tempMap.put("Sensor_" + i, 50 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String s : tempMap.keySet()) {
                    Double temp = tempMap.get(s);

                    double newTemp = temp + random.nextGaussian();

                    sourceContext.collect(new SensorReading(s, System.currentTimeMillis(), newTemp));

                    tempMap.put(s, newTemp);
                }
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
