package xuwei.tech.window;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 测试ProcessWinFunction
 *
 * @author dajiangtai
 * @create 2019-06-11-18:37
 */
public class CountWindowTest {

    public static void main(String[] args) throws Exception{
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStream<Tuple3<String,String,Long>> input = env.fromElements(ENGLISH);

        //求各班级英语成绩平均分
        DataStream<Double> avgScore = input.keyBy(0)
                .countWindow(2)   //到2个元素就触发
                .process(new MyProcessWindowFunction());
        avgScore.print();
        env.execute("TestProcessWinFunctionOnWindow");

    }

    //ProcessWindowFunction  获得一个包含窗口所有元素的可迭代器，以及一个具有时间和状态信息访问权的上下文对象
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String,String,Long>,Double, Tuple, GlobalWindow>{
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Long>> iterable, Collector<Double> out) throws Exception {
            long sum = 0;
            long count = 0;
            for (Tuple3<String,String,Long> in :iterable){
                sum+=in.f2;
                count++;
            }
            out.collect((double)(sum/count));
        }
    }

    public static final Tuple3[] ENGLISH = new Tuple3[]{
            Tuple3.of("class1","张三",100L),
            Tuple3.of("class1","李四",78L),
            Tuple3.of("class1","王五",99L),
            Tuple3.of("class2","赵六",81L),
            Tuple3.of("class2","小七",59L),
            Tuple3.of("class2","小八",97L),
    };
}
