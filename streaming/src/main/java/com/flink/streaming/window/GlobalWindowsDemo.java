package com.flink.streaming.window;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class GlobalWindowsDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input = env.addSource(new DataSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(20)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                        return element.f2;
                    }
                }).keyBy(0);
        input.window(GlobalWindows.create()).trigger(new Trigger<Tuple3<String, Integer, Long>, GlobalWindow>() {
            private AtomicLong num = new AtomicLong(0);
            @Override
            public TriggerResult onElement(Tuple3<String, Integer, Long> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
                if (num.incrementAndGet() > 2){
                    num.set(0);
                    return TriggerResult.FIRE;
                }
                //                maxTimestamp = element.f2 - 2000L;
//
//                if (maxTimestamp <= ctx.getCurrentWatermark()) {
//                    // if the watermark is already past the window fire immediately
//                    return TriggerResult.FIRE;
//                } else {
//                    ctx.registerEventTimeTimer(maxTimestamp);
//                    return TriggerResult.CONTINUE;
//                }
                //ctx.registerEventTimeTimer(1582336804000L);
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                return null;
            }

            @Override
            public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                System.out.println("======");
                return TriggerResult.FIRE;
//                return time % 3==0  ?
//                        TriggerResult.FIRE :
//                        TriggerResult.CONTINUE;
            }

            @Override
            public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
                num.set(0);
            }
        }).apply(new WindowFunction<Tuple3<String, Integer, Long>, Object, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<Object> out) throws Exception {
                List<Tuple3<String, Integer, Long>> list = Lists.newArrayList(input);
                out.collect(list);
            }
        }).print();

        env.execute();
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, Integer, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
            Tuple3<String, Integer, Long>[] d = new Tuple3[]{
                    new Tuple3<>("foo", 1, 1582336800000L),
                    new Tuple3<>("foo", 2, 1582336801000L),
                    new Tuple3<>("foo", 3, 1582336804000L),
                    new Tuple3<>("foo", 4, 1582336805000L),
                    new Tuple3<>("foo", 5, 1582336811000L),
                    new Tuple3<>("foo", 6, 1582336815000L)
            };
            final long numElements = d.length;

            int i = 0;
            while (running && i < numElements) {
                long r = RandomUtils.nextLong(1, 5);
                Thread.sleep(r * 1000L);
                ctx.collect(d[i]);
                System.out.println("Sand data:" + d[i]);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
