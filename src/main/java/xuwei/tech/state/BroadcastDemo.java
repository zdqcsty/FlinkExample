/*
package xuwei.tech.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class BroadcastDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.printf(env.getParallelism() + "");

        env.setParallelism(1);

        final DataStreamSource<List<String>> broadcastSource = env.addSource(new JdbcReader());

        //broadcast流
        DataStreamSource<String> mainStream = env.socketTextStream("10.130.7.202", 10007);

        final SingleOutputStreamOperator<Tuple2<String, String>> resuleStream = mainStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String s0 = value.split(",")[0];
                String s1 = value.split(",")[1];
                return new Tuple2<String, String>(s0, s1);
            }
        });


        MapStateDescriptor<String, String> broadCastConfigDescriptor = new MapStateDescriptor<>("broadCastConfig",
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        BroadcastStream<String> broadcastStream = broadcastSource.map(new MapFunction<List<String>, String>() {
            @Override
            public String map(List<String> value) throws Exception {
                for (String str : value) {
                    return str;
                }
                return null;
            }
        }).broadcast(broadCastConfigDescriptor);

        resuleStream.keyBy(0).rebalance().connect(broadcastStream).process(new KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, String>() {
            private transient MapState<String, Integer> counterState;
            int length = 5;
            // 必须和上文的 broadCastConfigDescriptor 一致，否则报 java.lang.IllegalArgumentException: The requested state does not exist 的错误
            private final MapStateDescriptor<String, String> broadCastConfigDescriptor = new MapStateDescriptor<>("broadCastConfig", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
            private final MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("counter", String.class, Integer.class);

            @Override
            public void open(Configuration parameters) throws Exception {
                counterState = getRuntimeContext().getMapState(descriptor);
            }


            @Override
            public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                System.out.println("+++++");

                final ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadCastConfigDescriptor);

                final String s = broadcastState.get(null);

                System.out.println(s);
            }

            */
/**
             * 这里处理广播流的数据
             * *//*

            @Override
            public void processBroadcastElement(String broadcast, Context ctx, Collector<String> out) throws Exception {

                System.out.println("--------");
                System.out.println(broadcast);

                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadCastConfigDescriptor);

                //清空状态
                broadcastState.clear();

                broadcastState.put(null, broadcast);

              */
/*  BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadCastConfigDescriptor);
                // 前面说过，BroadcastState 类似于 MapState.这里的 broadcastStateKey 是随意指定的 key, 用于示例
                // 更新广播流的规则到广播状态: BroadcastState
                System.out.println("------------------");

                final Iterator<Map.Entry<String, String>> iterator = broadcastState.iterator();
                while (iterator.hasNext()) {
                    final String key = iterator.next().getKey();
                    final String value = iterator.next().getValue();
                    System.out.printf(key + "---------" + value);
                }*//*

            }
        }).print();

        env.execute("BroadCastWordCountExample");
    }

}
*/
