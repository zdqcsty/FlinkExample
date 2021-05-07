package xuwei.tech.bingxing;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BingXingTest {

    public static void main(String[] args) throws Exception {

        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("10.130.7.202", 10008, "\n");

        Test1 test1 = new Test1();
        Test2 test2 = new Test2();

        test1.test1Execute(text);
        test2.test2Execute(text);

        env.execute("more job user one source");
    }

}


