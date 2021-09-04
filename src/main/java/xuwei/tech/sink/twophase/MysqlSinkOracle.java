package xuwei.tech.sink.twophase;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.Random;

public class MysqlSinkOracle {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.130.7.206:9092");
        properties.setProperty("group.id", "demoaaa");

        try {
            // 创建Flink执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(1);

            //checkpoint配置
            //定时执行checkpoint的时间
            env.enableCheckpointing(2000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//            env.getCheckpointConfig().setCheckpointTimeout(60000);
//            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            //设置statebackend
//            StateBackend backend = new FsStateBackend("hdfs:///test001/zgh/checkpoint");
            StateBackend backend = new FsStateBackend("file:///E:\\test");
            env.setStateBackend(backend);


            FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("ceshi", new SimpleStringSchema(),properties);

            consumer.setStartFromEarliest();

            DataStream<String> sourceStream = env.addSource(consumer).returns(String.class);

            Random random = new Random(20);

            final SingleOutputStreamOperator<String> sinkStream = sourceStream.map(new MapFunction<String, String>() {
                @Override
                public String map(String value) throws Exception {
                    final String i = random.nextInt() + "";
                    System.out.println("------------"+i);
                    return "insert into test_zgh.test values ('" + i + "')";
                }
            });

            // TODO 在这里决定是否调用两阶段提交逻辑
//            sinkStream.addSink(new OracleSinkFunction());
            sinkStream.addSink(new OracleTwoPhaseCommitSink());
            env.execute("aaa");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
