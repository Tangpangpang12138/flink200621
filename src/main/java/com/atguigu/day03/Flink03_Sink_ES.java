package com.atguigu.day03;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author TangZC
 * @create 2020-11-18 14:47
 */
public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("sensor");

        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

        ElasticsearchSink<String> build = new ElasticsearchSink.Builder<String>(httpHosts, new MyEsSinkFunc()).build();

        inputDS.addSink(build);

        env.execute();
    }

    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<String> {

        @Override
        public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            String[] split = s.split(",");

            HashMap<String, String> source = new HashMap<String, String>();
            source.put("id",split[0]);
            source.put("ts",split[1]);
            source.put("temp", split[2]);

            IndexRequest source1 = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
//                    .id(split[0])
                    .source(source);

            requestIndexer.add(source1);
        }
    }
}
