package org.apache.flink.ProcessWindowFunction;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class CSProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Integer>, Tuple3<String,Integer,Long>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
        Tuple2<String,Integer> t = elements.iterator().next();
        out.collect(new Tuple3<>(t.f0,t.f1,context.window().getStart()));
    }
}
