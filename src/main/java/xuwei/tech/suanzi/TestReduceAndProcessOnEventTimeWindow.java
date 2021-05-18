package xuwei.tech.suanzi;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class TestReduceAndProcessOnEventTimeWindow {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        DataStream<Tuple3<String, Long, Integer>> text = env.socketTextStream("10.130.7.202", 10008, "\n")
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String value) throws Exception {
                        final String[] split = value.split(",");
                        return new Tuple3<String, Long, Integer>(split[1], Long.parseLong(split[0]), Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, Long, Integer> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }));


        //ReduceFunction 两个输入的元素进行合并来生成相同类型的输出元素   进行增量聚合的  最后只留下一个
        text.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Long, Integer>>() {
                            @Override
                            public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> value1, Tuple3<String, Long, Integer> value2) throws Exception {
                                return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
                            }
                        },
                        //有很多这样的场景，就是我们自己在聚合的时候，keyby完之后拿不到event time  这个时候就可以在ProcessWindowFunction 方法中进行拿到  然后输出
                        new ProcessWindowFunction<Tuple3<String, Long, Integer>, Object, Tuple, TimeWindow>() {
                            @Override
                            public void process(Tuple tuple, Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<Object> out) throws Exception {

                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                String end = sdf.format(context.window().getEnd());
                                String start = sdf.format(context.window().getStart());
                                out.collect("start -- end is " + start + " - " + end);
                            }
                        }
                ).print();

        env.execute("TestReduceFunctionOnWindow");
    }

}
