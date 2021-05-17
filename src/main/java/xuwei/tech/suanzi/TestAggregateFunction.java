package xuwei.tech.suanzi;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestAggregateFunction {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> text = env.socketTextStream("10.130.7.202", 10008, "\n")
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                });

        text.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {

                    //生成累加器，启动一个新的聚合,负责迭代状态的初始化
                    @Override
                    public Integer createAccumulator() {
                        return 2;
                    }

                    //对于数据的每条数据，和迭代数据的聚合的具体实现
                    @Override
                    public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                        return value.f1 + accumulator;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    //AggregateFunction中的merge方法仅SessionWindow会调用该方法，如果time window是不会调用的，merge方法即使返回null也是可以的。
                    // 可以看看官方的文档中的描述和结合翻看源码就可以搞清楚了
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a - b;
                    }
                }).print();

        env.execute("TestReduceFunctionOnWindow");
    }

}
