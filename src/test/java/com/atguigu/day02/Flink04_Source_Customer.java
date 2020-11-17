package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author TangZC
 * @create 2020-11-17 14:36
 */
public class Flink04_Source_Customer {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new CustomerSource1());

    }


    private static class CustomerSource1 implements org.apache.flink.streaming.api.functions.source.SourceFunction<Object> {
        @Override
        public void run(SourceContext<Object> sourceContext) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }
}
