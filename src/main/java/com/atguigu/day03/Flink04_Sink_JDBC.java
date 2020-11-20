package com.atguigu.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author TangZC
 * @create 2020-11-18 18:03
 */
public class Flink04_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("sensor");

        //将数据写入Mysql
        inputDS.addSink(new JDBCSink());

        //执行任务
        env.execute();
    }

    public static class JDBCSink extends RichSinkFunction<String> {

      //申明MySQL相关的属性信息
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
        preparedStatement = connection.prepareStatement("INSERT INTO sensor(id,temp) VALUES(?,?) ON DUPLICATE KEY UPDATE temp=?");

        }


        @Override
        public void invoke(String value, Context context) throws Exception {
            //分割数据
            String[] split = value.split(",");
            //给预编译SQL赋值
            preparedStatement.setString(1, split[0]);
            preparedStatement.setDouble(2,Double.parseDouble(split[2]));
            preparedStatement.setDouble(3,Double.parseDouble(split[2]));

            //执行
            preparedStatement.execute();
        }


        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
