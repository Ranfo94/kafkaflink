package org.apache.flink;

import org.apache.flink.ProcessWindowFunction.CSProcessAllWindowFunction;
import org.apache.flink.ProcessWindowFunction.CSProcessWindowFunction;
import org.apache.flink.ProcessWindowFunction.CSReduceFunction;
import org.apache.flink.ProcessWindowFunction.TryWindowFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.entity.Comment;
import org.apache.flink.schema.CommentSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

public class StreamingConsumer {

    public static void main(String[] args) throws Exception {

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "consumerGroup1");
        properties.setProperty("auto.offset.reset", "earliest");
        DataStream<Comment> stream = env
                .addSource(new FlinkKafkaConsumer<>("cs", new CommentSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Comment>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Comment element) {
                        return element.getCreateDate()*1000;
                    }
                });
        stream.filter(Objects::nonNull);

        DataStream<String> mappedIn = stream.map(new MapFunction<Comment, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Comment value) throws Exception {
                return new Tuple2<>(value.getArticleID(),1);
            }
        })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .sum(1)
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
                .process(new CSProcessAllWindowFunction());

        mappedIn.print().setParallelism(1);
        env.execute("Consumer Streaming");
    }
}
