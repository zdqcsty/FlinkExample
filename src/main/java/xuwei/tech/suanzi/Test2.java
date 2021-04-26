package xuwei.tech.suanzi;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;

public class Test2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Score> tupleStream = env.fromElements(
                new Score("Li", "English", 90), new Score("Wang", "English", 88), new Score("Li", "Math", 85),
                new Score("Wang", "Math", 92), new Score("Liu", "Math", 91), new Score("Liu", "English", 87));

        tupleStream.keyBy(new KeySelector<Score, Object>() {
            @Override
            public String getKey(Score value) throws Exception {
                return value.getName();
            }
        }).reduce(new AggregationFunction<Score>() {
            @Override
            public Score reduce(Score value1, Score value2) throws Exception {
                return new Score(value1.getName(),"SUM",value2.getScore()+value1.getScore());
            }
        }).print();

        env.execute("flink TumblingWindow");
    }

}
