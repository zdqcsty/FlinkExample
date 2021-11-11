package xuwei.tech.test.car;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.time.Duration;

public class CarCeshi {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FlinkKafkaConsumer010 consumer = FlinkConsumeKafkaCar.getConsumer();

        SingleOutputStreamOperator<Car> inputstream = env
                .addSource(consumer).name("Main add upload kafka source").uid("Main add upload kafka source")
                .returns(Car.class).name("Main upload return").uid("Main upload return");

        //checkpoint配置
        //定时执行checkpoint的时间
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        设置statebackend
        StateBackend backend = new FsStateBackend("hdfs:///test001/zgh/checkpointaaa");
        env.setStateBackend(backend);

        env.setRestartStrategy(RestartStrategies.noRestart());


        SingleOutputStreamOperator<Car> resultStream = inputstream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Car>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Car>() {
                    @Override
                    public long extractTimestamp(Car element, long recordTimestamp) {
                        return System.currentTimeMillis();
                    }
                })).name("timestamp").uid("timestamp");

        resultStream.map(new MapFunction<Car, CarCounter>() {
            @Override
            public CarCounter map(Car value) throws Exception {

                CarCounter counter = new CarCounter();
                counter.setVin(value.vin);
                counter.setCounter(1);
                counter.setFactory(value.factory);
                return counter;
            }
        }).keyBy(new KeySelector<CarCounter, String>() {
            @Override
            public String getKey(CarCounter value) throws Exception {
                return String.valueOf(value.getFactory());
            }
        }).sum("counter").addSink(new JdbcSink());

/*
        System.out.println("--------------------");
        stringStringKeyedStream.print();
        System.out.println("--------------------");*/
/*

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
*/

        env.execute("ccc");
    }


 /*   public static void doSum(SingleOutputStreamOperator resultStream){
        resultStream.keyBy(resultStream.)

    }*/

}
