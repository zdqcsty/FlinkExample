package xuwei.tech.watermark;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * trigger 的定义表明   他是用来触发window的操作。
 * 当没有window的算子操作时，flink相当于来一个计算一个。
 */

public class WatermarkWithoutWindow {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 添加数据源
        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        DataStream stream = text
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(1))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String element, long recordTimestamp) {
                                        return Long.parseLong(element);
                                    }
                                }
                        ));


        stream.map(new MapFunction<String, String>() {

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();

        env.execute("flink TumblingWindow");
    }

}
