package xuwei.tech.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * # _*_ coding:utf-8 _*_
 * # Author:xiaoshubiao
 * # Time : 2020/12/11 14:09
 **/
public class state_test2 {
    public static void main(String[] args) throws Exception {
        class stateFalg {
            String name;
            long count;
        }
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localhost = executionEnvironment.socketTextStream("10.130.7.202", 10008);
        // 输入a,1这样的数据，计算连续两个相同key的数量差值
        // 差值不能大于10
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = localhost.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<>(split[0], Integer.valueOf(split[1]));
                    }
                }
        );
        map.keyBy(0).flatMap(
                // 参数1：是输入的数据类型
                //参数2：是报警时显示的数据类型
                new RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, String>>() {
                    ValueState<stateFalg> state1;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //创建一个状态值
                        ValueStateDescriptor<stateFalg> state = new ValueStateDescriptor<>("state", stateFalg.class);
                        state1 = getRuntimeContext().getState(state);
                    }

                    @Override
                    public void flatMap(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple2<String, String>> collector) throws Exception {
                        // 判断状态值是否为空（状态默认值是空）
                        if (state1.value() == null) {
                            stateFalg sFalg = new stateFalg();
                            sFalg.name = stringIntegerTuple2.f0;
                            sFalg.count = stringIntegerTuple2.f1;
                            state1.update(sFalg);
                        }
                        stateFalg value = state1.value();
                        if (Math.abs(value.count - stringIntegerTuple2.f1) > 10) {
                            collector.collect(new Tuple2<String, String>("数量超出警戒值", stringIntegerTuple2.f0));
                        }
                        value.name = stringIntegerTuple2.f0;
                        value.count = stringIntegerTuple2.f1;
                        // 更新状态值
                        state1.update(value);
                    }
                }
        ).print();


        executionEnvironment.execute();
    }
}
