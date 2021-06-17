package xuwei.tech.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 执行方式
 * 开启10007 端口  输入length,6  或者 length,10
 *
 * 开启10008 端口  输入no,1,1 或者  cdacdaicda,1,1
 */

public class BroadcastStateTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataSource = env.socketTextStream("10.130.7.202", 10008);

        //数据流
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> tupleData = dataSource.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple3<>(String.valueOf(split[0]), Integer.valueOf(split[1]), Long.valueOf(split[2]));
            }
        });

        //broadcast流
        DataStreamSource<String> broadCast = env.socketTextStream("10.130.7.202", 10007);

        MapStateDescriptor<String, Map<String, Object>> broadCastConfigDescriptor = new MapStateDescriptor<>("broadCastConfig",
                BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(String.class, Object.class));
        // e.g. {"length":5}

        BroadcastStream<Map<String, Object>> broadcastStream = broadCast.map(new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String value) throws Exception {

                final String[] split = value.split(",");
                Map map = new HashMap<String, Object>();
                map.put(split[0], split[1]);
                return map;
            }
        }).broadcast(broadCastConfigDescriptor);

        tupleData.keyBy(0).connect(broadcastStream).process(new KeyedBroadcastProcessFunction<String, Tuple3<String, Integer, Long>, Map<String, Object>, Tuple2<String, Integer>>() {
            private transient MapState<String, Integer> counterState;
            int length = 5;
            // 必须和上文的 broadCastConfigDescriptor 一致，否则报 java.lang.IllegalArgumentException: The requested state does not exist 的错误
            private final MapStateDescriptor<String, Map<String, Object>> broadCastConfigDescriptor = new MapStateDescriptor<>("broadCastConfig", BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(String.class, Object.class));
            private final MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("counter", String.class, Integer.class);

            @Override
            public void open(Configuration parameters) throws Exception {
                counterState = getRuntimeContext().getMapState(descriptor);
//                 MapState<String, Map<String, Object>> mapState = getRuntimeContext().getMapState(broadCastConfigDescriptor);
            }

            /**
             * 这里处理数据流的数据
             * */
            @Override
            public void processElement(Tuple3<String, Integer, Long> value, ReadOnlyContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                /**
                 * 这里之只能获取到 ReadOnlyBroadcastState，因为 Flink 不允许在这里修改 BroadcastState 的状态
                 * */
                // 从广播状态中获取规则
                ReadOnlyBroadcastState<String, Map<String, Object>> broadcastState = ctx.getBroadcastState(broadCastConfigDescriptor);
                if (broadcastState.contains("broadcastStateKey")) {
                    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
                    System.out.println(broadcastState.get("broadcastStateKey"));
                    System.out.println(broadcastState.get("broadcastStateKey").get("length"));

                    length = Integer.valueOf((String) broadcastState.get("broadcastStateKey").get("length"));
                }
                if (value.f0.length() > length) {
                    return;
                }
                if (counterState.contains(value.f0)) {
                    counterState.put(value.f0, counterState.get(value.f0) + value.f1);
                } else {
                    counterState.put(value.f0, value.f1);
                }
                out.collect(new Tuple2<>(value.f0, counterState.get(value.f0)));
            }

            /**
             * 这里处理广播流的数据
             * */
            @Override
            public void processBroadcastElement(Map<String, Object> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (!value.containsKey("length")) {
                    return;
                }

                /*ctx.applyToKeyedState(broadCastConfigDescriptor, (key, state) -> {
                     // 这里可以修改所有 broadCastConfigDescriptor 描述的 state
                });*/
                /** 这里获取 BroadcastState，BroadcastState 包含 Map 结构，可以修改、添加、删除、迭代等
                 * */
                BroadcastState<String, Map<String, Object>> broadcastState = ctx.getBroadcastState(broadCastConfigDescriptor);
                // 前面说过，BroadcastState 类似于 MapState.这里的 broadcastStateKey 是随意指定的 key, 用于示例
                // 更新广播流的规则到广播状态: BroadcastState
                System.out.println(value);
                if (broadcastState.contains("broadcastStateKey")) {
                    Map<String, Object> oldMap = broadcastState.get("broadcastStateKey");
                    System.out.println("-------------");
                    System.out.println(oldMap);
                } else {
                }
                broadcastState.put("broadcastStateKey", value);
            }
        }).print();

        env.execute("BroadCastWordCountExample");
    }
}