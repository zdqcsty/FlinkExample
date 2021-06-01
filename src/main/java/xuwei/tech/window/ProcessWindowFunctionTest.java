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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessWindowFunctionTest {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        KeyedStream<Tuple2<Long, String>, String> KeyedStream = text.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2(Long.parseLong(split[0]), split[1]);
            }
        }).keyBy(new KeySelector<Tuple2<Long, String>, String>() {
            @Override
            public String getKey(Tuple2<Long, String> value) throws Exception {
                return value.f1;
            }
        });

/*        KeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .process(new ProcessWindowFunction<Tuple2<Long, String>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<Long, String>> elements, Collector<String> out) throws Exception {
                        System.out.println(s);
                        out.collect("aaaa");
                    }
                }).print();*/


        KeyedStream.process(new KeyedProcessFunction<String, Tuple2<Long, String>, String>() {
            @Override
            public void processElement(Tuple2<Long, String> value, Context ctx, Collector<String> out) throws Exception {
                System.out.println(value.f1);
                out.collect("aaa");
            }
        }).print();


        env.execute("flink TumblingWindow");
    }

}
