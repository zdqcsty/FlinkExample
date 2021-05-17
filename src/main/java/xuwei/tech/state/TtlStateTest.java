package xuwei.tech.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TtlStateTest {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        // 输入的活动数据转换
        DataStream<Tuple2<String, Integer>> windowCount =
                text.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) throws Exception {
                        return new Tuple2<String, Integer>(line, 1);
                    }
                });

        windowCount.keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Integer>() {

                    ValueState<Integer> state = null;

                    int i=0;
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        /**
                         * 目前TTL只是针对于 process time
                         * 太清楚的目前就是各个状态的
                         */
                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(100))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();

                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("text state", Integer.class);
                        stateDescriptor.enableTimeToLive(ttlConfig);
                        state = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<String, Integer> value, Collector<Integer> out) throws Exception {
                        System.out.println(state.value());
                        i++;
                        state.update(i);
                        System.out.println(state.value());
                        out.collect(state.value());
                    }
                }).print();

        //启动(这个异常不建议try...catch... 捕获,因为它会抛给上层flink,flink根据异常来做相应的重启策略等处理)
        env.execute("StreamWordCount");

    }

}
