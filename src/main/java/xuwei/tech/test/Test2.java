package xuwei.tech.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localhost = executionEnvironment.socketTextStream("10.130.7.202", 10008);

        localhost.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String s0 = value.split(",")[0];
                String s1 = value.split(",")[1];
                return new Tuple2<String, String>(s0, s1);
            }
        }).keyBy(0).flatMap(
                // 参数1：是输入的数据类型
                //参数2：是报警时显示的数据类型
                new RichFlatMapFunction<Tuple2<String, String>, String>() {
                    MapState<String, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        MapStateDescriptor<String, String> productRank = new MapStateDescriptor<String, String>("productRank", String.class, String.class);
                        mapState = getRuntimeContext().getMapState(productRank);
                    }

                    @Override
                    public void flatMap(Tuple2<String, String> tuple, Collector<String> collector) throws Exception {
                        mapState.put(tuple.f0, tuple.f1);
                        System.out.println(mapState.values());
                        collector.collect(tuple.f1);
                    }
                }
        ).print();

        executionEnvironment.execute("aaa");
    }
}