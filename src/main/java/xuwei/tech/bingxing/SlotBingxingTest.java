package xuwei.tech.bingxing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class SlotBingxingTest {

    //静态方法在 taskmanager中保存的，taskmanager 就是一个JVM实例
    public static Map<Integer, Integer> map;

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<Integer, Integer, Integer>> tupleStream = env.fromElements(
                new Tuple3<Integer, Integer, Integer>(1, 2, 3),
                new Tuple3<Integer, Integer, Integer>(0, 1, 1),
                new Tuple3<Integer, Integer, Integer>(0, 2, 2),
                new Tuple3<Integer, Integer, Integer>(1, 0, 6),
                new Tuple3<Integer, Integer, Integer>(1, 1, 7),
                new Tuple3<Integer, Integer, Integer>(1, 2, 8));

        tupleStream.keyBy(new KeySelector<Tuple3<Integer, Integer,Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple3<Integer, Integer,Integer> value) throws Exception {
                return value.f0;
            }
        }).flatMap(new RichFlatMapFunction<Tuple3<Integer, Integer, Integer>, Object>() {
            @Override
            public void flatMap(Tuple3<Integer, Integer, Integer> value, Collector<Object> out) throws Exception {

                map.put(value.f2,value.f0);
                System.out.println(map);
                out.collect("aaa");
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                map = new HashMap<>();
            }

        }).print();

        env.execute("flink TumblingWindow");
    }

}
