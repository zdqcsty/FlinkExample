package xuwei.tech.bingxing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class Test2 {

    public void test2Execute(DataStreamSource source) {
        source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if ("demo".equals(value)) {
//                    System.out.println("+++++++" + value);
//                    throw new Exception("exception");
                }
                return new Tuple2<>(value, 1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1).print();
    }
}
