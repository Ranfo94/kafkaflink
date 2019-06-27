package org.apache.flink.ProcessWindowFunction;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TryWindowFunction extends ProcessWindowFunction<Tuple2<String,Integer>,String, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
        ArrayList<Tuple2<String,Integer>> tupleList = new ArrayList<>();
        for (Tuple2<String,Integer> elem : elements){
            tupleList.add(elem);
        }
        out.collect("TIMESTAMP : "+context.window().getStart() + " \nWINDOW LIST: "+tupleList.toString());
    }
}
