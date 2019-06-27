package org.apache.flink.ProcessWindowFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class CSProcessAllWindowFunction extends ProcessAllWindowFunction<Tuple2<String,Integer>,String, TimeWindow> {
    @Override
    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
        ArrayList<Tuple2<String,Integer>> list = new ArrayList<>();
        for (Tuple2<String,Integer> elem : elements){
            list.add(elem);
        }
        Collections.sort(list, new Comparator<Tuple2<String, Integer>>() {
            @Override
            public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
                int v1 = t1.f1;
                int v2 = t2.f1;
                return v2-v1;
            }
        });
        if (list.size() >= 3){
            out.collect("WINDOWS START: "+context.window().getStart()+ "\n" +
                    "RANK 1: "+list.get(0)+"\n"+
                    "RANK 2: "+list.get(1)+"\n"+
                    "RANK 3: "+list.get(2));
        }if (list.size()==2){
            out.collect("WINDOWS START: "+context.window().getStart()+ "\n" +
                    "RANK 1: "+list.get(0)+"\n"+
                    "RANK 2: "+list.get(1)+"\n"+
                    "RANK 3: NON CS");
        }if (list.size()==1){
            out.collect("WINDOWS START: "+context.window().getStart()+ "\n" +
                    "RANK 1: "+list.get(0)+"\n"+
                    "RANK 2: NON CS"+"\n"+
                    "RANK 3: NON CS");
        }else {
            out.collect("WINDOWS START: "+context.window().getStart()+ "\n" +
                    "RANK 1: NON CS"+"\n"+
                    "RANK 2: NON CS"+"\n"+
                    "RANK 3: NON CS");
        }

    }
}
