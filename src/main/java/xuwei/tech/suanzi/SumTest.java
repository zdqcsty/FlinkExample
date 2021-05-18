package xuwei.tech.suanzi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SumTest {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        DataStream<Tuple3<String, String, Integer>> intData = text.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple3<String, String, Integer>(split[0], split[1], 1);
            }
        });

        intData.keyBy(0)
                //随着数据的流入，这个数据是实时的累加的
                //sum 相关的话  如果有三个字段  根据一个进行keyby  另一个进行sum的话，还剩下一个的话会复制之前，就是一个值
                .sum(2).print();

        env.execute("flink TumblingWindow");
    }

}
