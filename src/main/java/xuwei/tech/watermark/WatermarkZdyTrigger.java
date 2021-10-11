package xuwei.tech.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WatermarkZdyTrigger {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 添加数据源
        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> streamOperator = text.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple3<>(split[0], split[1], Integer.parseInt(split[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofMillis(1))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple3<String, String, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Integer> element, long recordTimestamp) {
                                return Long.parseLong(element.f0);
                            }
                        }
                ));

        streamOperator.keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .trigger(new CustomTrigger())
                .reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
//                        System.out.println(value1);
//                        System.out.println(value2);
                        return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                }).print();

        env.execute("flink TumblingWindow");
    }

}
