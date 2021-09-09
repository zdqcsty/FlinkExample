package xuwei.tech.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkConsumeKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FlinkKafkaConsumer consumer = getConsumer();

        SingleOutputStreamOperator<String> inputstream = env
                .addSource(consumer).name("Main add upload kafka source").uid("Main add upload kafka source")
                .returns(String.class).name("Main upload return").uid("Main upload return");

        inputstream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();

        env.execute("aaaa");
    }

    public static FlinkKafkaConsumer getConsumer() {

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "10.130.7.206:9092");
        properties.setProperty("group.id", "demoacdacdac");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        return consumer;
    }
}
