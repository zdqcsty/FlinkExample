package xuwei.tech.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class StreamingKafkaSink {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        //checkpoint配置
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StateBackend backend = new FsStateBackend("hdfs://hebing2.novalocal:8020/user/zgh/checkpoint");

//        设置statebackend
        env.setStateBackend(backend);

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        String brokerList = "10.130.7.202:9092";
        String topic = "demo";

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", brokerList);

        //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
        //设置事务超时时间
        //prop.setProperty("transaction.timeout.ms",60000*15+"");

        //第二种解决方案，设置kafka的最大事务超时时间

        //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());

        //使用仅一次语义的kafkaProducer
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer(topic, new SimpleStringSchema(), prop);
        text.addSink(myProducer);

        env.execute("StreamingFromCollection");
    }
}
