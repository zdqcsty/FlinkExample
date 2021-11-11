package xuwei.tech;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;

public class SavePointTest {

    private  static final String CHECKPOINT_URI="hdfs:///test001/zgh/checkpoint";
    private  static final String SAVEPOINT_URI="hdfs:///test001/zgh/savepoint";

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        //checkpoint配置
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //重启策略机制
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.minutes(60),Time.seconds(30)));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        URI checkPoint=new URI(CHECKPOINT_URI);
        URI savePoint=new URI(SAVEPOINT_URI);

        StateBackend backend = new FsStateBackend(checkPoint,savePoint);


//        设置statebackend
        env.setStateBackend(backend);

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        text.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if ("demo".equals(value)) {
                    System.out.println("+++++++"+value);
                    throw new Exception("exception");
                }
                return new Tuple2<>(value, 1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1).print();

        env.execute("StreamingFromCollection");
    }
}
