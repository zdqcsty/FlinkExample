package xuwei.tech.window.numberstream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Test1 {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        DataStream<Integer> initStream = env.addSource(new IntegerGenerator());

        initStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Integer> input, Collector<Integer> output) throws Exception {
                        int sum = 0;
                        for (int i : input) {
                            sum += i;
                        }
                        output.collect(sum);
                    }
                })
                .print();
        env.execute("flink TumblingWindow");
    }

}
