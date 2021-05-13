package xuwei.tech.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class PrintSink {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });


        inputMap.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<String, Long>, String>() {


            ListState<String> list = null;
            ValueState<Integer> size = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ListStateDescriptor<String> listStatedesc = new ListStateDescriptor<String>("test", String.class);
                list = getRuntimeContext().getListState(listStatedesc);

                ValueStateDescriptor<Integer> valueStatedesc = new ValueStateDescriptor<Integer>("", Integer.class, 0);
                size = getRuntimeContext().getState(valueStatedesc);
            }

            @Override
            public void flatMap(Tuple2<String, Long> value, Collector<String> out) throws Exception {
                System.out.println(size.value());
                if (size.value() > 3) {
                    //证明了一点   如果out 没有collect的话，是不会输出任何东西的
                    out.collect("haahahahahah");
                } else {
                    list.add(value.f0);
                    size.update(size.value() + 1);
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

        }).print();

        env.execute("flink TumblingWindow");
    }
}
