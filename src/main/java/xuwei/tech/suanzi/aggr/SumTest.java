package xuwei.tech.suanzi.aggr;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SumTest {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        DataStream<Tuple2<Integer, Integer>> intData = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1, Integer.parseInt(value));
            }
        });

        intData.keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
                return value.f0;
            }
        })
                //随着数据的流入，这个数据是实时的累加的
                .sum(1).print();

        env.execute("flink TumblingWindow");
    }

}
