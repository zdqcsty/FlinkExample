package xuwei.tech.suanzi.aggr;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<Integer, Integer, Integer>> tupleStream = env.fromElements(
                new Tuple3<Integer, Integer, Integer>(1, 2, 3),
                new Tuple3<Integer, Integer, Integer>(0, 1, 1),
                new Tuple3<Integer, Integer, Integer>(0, 2, 2),
                new Tuple3<Integer, Integer, Integer>(1, 0, 6),
                new Tuple3<Integer, Integer, Integer>(1, 1, 7),
                new Tuple3<Integer, Integer, Integer>(1, 2, 8));

        tupleStream.keyBy(new KeySelector<Tuple3<Integer, Integer, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple3<Integer, Integer, Integer> value) throws Exception {
                return value.f0;
            }
        }).maxBy(2).print();

        env.execute("flink TumblingWindow");
    }
}
