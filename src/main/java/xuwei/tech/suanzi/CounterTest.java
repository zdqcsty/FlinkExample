package xuwei.tech.suanzi;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CounterTest {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据来源
        DataStream<Tuple2<String, Integer>> text = env.socketTextStream("10.130.7.207", 10008)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                });


        //operate
        final SingleOutputStreamOperator<String> productRank = text.keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Integer>, String>() {

                    //第一步：定义累加器
                    private IntCounter numLines = new IntCounter();

                    private MapState<String, Void> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        MapStateDescriptor<String, Void> productRank = new MapStateDescriptor<String, Void>("productRank", String.class, Void.class);
                        mapState = getRuntimeContext().getMapState(productRank);

                        //第二步：注册累加器
                        getRuntimeContext().addAccumulator("num-lines", numLines);
                    }

                    @Override
                    public String map(Tuple2<String, Integer> s) throws Exception {

                        if (!mapState.contains(s.f0)) {
                            numLines.add(1);
                        }
                        return numLines.getLocalValue().toString();
                    }
                });

        //执行
        env.execute("socketTest");
    }
}