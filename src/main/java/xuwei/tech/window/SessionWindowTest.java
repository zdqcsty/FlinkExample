package xuwei.tech.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class SessionWindowTest {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final int parallelism = env.getParallelism();
        System.out.println(parallelism);

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        final KeyedStream<Tuple2<Long, String>, String> tuple2StringKeyedStream = text.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2(Long.parseLong(split[0]), split[1]);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple2<Long, String> element, long recordTimestamp) {
                        return element.f0;
                    }
                }))
                .keyBy(new KeySelector<Tuple2<Long, String>, String>() {
                    @Override
                    public String getKey(Tuple2<Long, String> value) throws Exception {
                        return value.f1;
                    }
                });


        final int parallelism1 = tuple2StringKeyedStream.getParallelism();
        System.out.println(parallelism1);


        tuple2StringKeyedStream .window(EventTimeSessionWindows.withGap(Time.milliseconds(6000))).sum(0).print().setParallelism(1);
               /* .process(new ProcessWindowFunction<Tuple2<Long, String>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<Long, String>> elements, Collector<String> out) throws Exception {
                        long end = context.window().getEnd();
                        long start = context.window().getStart();
                        System.out.println("window  start  end  is " + end + "---" + start);
                        out.collect("window  start  end  is " + end + "---" + start);
                    }*/
//    }).
//
//    print();

        env.execute("flink TumblingWindow");

}
}
