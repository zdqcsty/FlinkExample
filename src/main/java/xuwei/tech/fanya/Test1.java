package xuwei.tech.fanya;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test1 {

    public static void main(String[] args) throws Exception {
/*        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        DataStream<String> initStream = env.addSource(new StringGenerator());

        initStream.addSink(new MysqlSink());

        env.execute("flink TumblingWindow");*/
    }
}
