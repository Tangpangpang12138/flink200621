package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.util.HashMap;
import java.util.Random;

/**
 * @author TangZC
 * @create 2020-11-17 12:36
 */
public class Flink04_Source_Customer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> mySourceDS = env.addSource(new CustomerSource());

        mySourceDS.print();

        env.execute();

    }

    public static class CustomerSource implements SourceFunction<SensorReading> {

        private boolean running = true;

        Random random = new Random();

        public void run(SourceContext<SensorReading> sourceContext) throws Exception {

            HashMap<String, Double> tempMap = new HashMap<String, Double>();

            for (int i = 0; i < 10; i++) {
                tempMap.put("Sensor_" + i, 50 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String id : tempMap.keySet()) {
                    Double temp = tempMap.get(id);

                    double newTemp = temp + random.nextGaussian();

                    sourceContext.collect(new SensorReading(id, System.currentTimeMillis(), newTemp));

                    tempMap.put(id, newTemp);

                }
                Thread.sleep(2000);
            }
        }

        public void cancel() {

            running = false;

        }
    }
}
