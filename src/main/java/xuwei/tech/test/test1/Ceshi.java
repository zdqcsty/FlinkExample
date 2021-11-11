package xuwei.tech.test.test1;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import xuwei.tech.fanya.MysqlSink;
import xuwei.tech.kafka.FlinkConsumeKafka;

import java.time.Duration;

public class Ceshi {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FlinkKafkaConsumer010 consumer = FlinkConsumeKafka.getConsumer();




        SingleOutputStreamOperator<String> inputstream = env
                .addSource(consumer).name("Main add upload kafka source").uid("Main add upload kafka source")
                .returns(String.class).name("Main upload return").uid("Main upload return");

        //checkpoint配置
        //定时执行checkpoint的时间
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        StateBackend backend = new FsStateBackend("hdfs:///test001/zgh/checkpoint");
        env.setStateBackend(backend);

        env.setRestartStrategy(RestartStrategies.noRestart());


        KeyedStream<String, String> stringStringKeyedStream = inputstream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return System.currentTimeMillis();
                    }
                })).name("timestamp").uid("timestamp").keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return "key";
            }
        });
/*
        System.out.println("--------------------");
        stringStringKeyedStream.print();
        System.out.println("--------------------");*/

        final SingleOutputStreamOperator<Integer> maxInputStream = stringStringKeyedStream.flatMap(new RichFlatMapFunction<String, Integer>() {

            ValueState<Integer> state = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("text state", Integer.class);
                state = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {
                final Integer integer = Integer.valueOf(value);

                System.out.println("into value is ---" + value);


                if (state.value() == null) {
                    state.update(0);
                }

                if (integer > state.value()) {
                    state.update(integer);
                    out.collect(integer);
                }
            }
        }).name("flatmap ceshi").uid("flatmap ceshi");

        maxInputStream.addSink(new MysqlSink()).name("mysql sink").uid("mysql sink");

        env.execute("aaa");
    }

}
