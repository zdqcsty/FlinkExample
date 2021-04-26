package xuwei.tech.window.socket;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class SocketWindowWatermark {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 添加数据源
        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        DataStream stream = text
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(4))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String element, long recordTimestamp) {
                                        return Long.parseLong(element);
                                    }
                                }
                        ));

//        DataStream<Integer> initStream = env.addSource(text);

        stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(2)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                        StringBuilder sb = new StringBuilder();
                        for (String e : elements) {
                            sb.append(e).append("\n");
                        }
                        System.out.println("当前窗口为" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window().getEnd()) + "---" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window().getStart()));

                        out.collect(sb.toString());
                    }
                }).print();
        env.execute("flink TumblingWindow");
    }

}
