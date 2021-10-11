package xuwei.tech.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SessionTest {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        // 输入的活动数据转换
        DataStream<Tuple3<String, Long, Integer>> windowCount =
                text.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String line) throws Exception {
                        String[] words = line.split(" ");
                        return new Tuple3<String, Long, Integer>(words[0], Long.valueOf(words[1]), 1);
                    }
                });

        // 描述 flink如何获取数据中的event时间戳进行判断
        // 描述延迟的watermark1秒
        DataStream<Tuple3<String, Long, Integer>> textWithEventTimeDStream =
                windowCount.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>
                        (Time.milliseconds(1000)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> stringLongIntegerTuple3) {
                        return stringLongIntegerTuple3.f1;
                    }
                }).setParallelism(1);

        // 按key分组，keyBy之后是分到各个分区再window去处理
        KeyedStream<Tuple3<String, Long, Integer>, Tuple> textKeyStream = textWithEventTimeDStream.keyBy(0);

        // 设置5秒的（会话窗口）活动时间间隔
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> windowStream =
                textKeyStream.window(EventTimeSessionWindows.withGap(Time.milliseconds(5000L))).sum(2);

        //3.调用Sink （Sink必须调用）
        windowStream.print("windows: ").setParallelism(1);
        //启动(这个异常不建议try...catch... 捕获,因为它会抛给上层flink,flink根据异常来做相应的重启策略等处理)
        env.execute("StreamWordCount");

    }


}
