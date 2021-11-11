package xuwei.tech.kafka;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;

public class KafkaTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        StateBackend backend = new FsStateBackend("hdfs://hebing1.novalocal:8020/user/zgh/flink/checkpoint");
//        env.setStateBackend(backend);
//        env.enableCheckpointing(6000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.130.7.202:9092");
        properties.setProperty("group.id", "demoaaa");
        FlinkKafkaConsumer010 consumer = new FlinkKafkaConsumer010("monitor-flink", new KafkaBeanDeserializat(), properties);
        consumer.setStartFromEarliest();
        DataStream<KafkaBean> stream = env.addSource(consumer).returns(KafkaBean.class)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<KafkaBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<KafkaBean>() {
                    @Override
                    public long extractTimestamp(KafkaBean element, long recordTimestamp) {
                        return element.getDate();
                    }
                }));

        stream.filter(new FilterFunction<KafkaBean>() {
            @Override
            public boolean filter(KafkaBean value) throws Exception {
    /*            System.out.println(value.toString());
                System.out.println(value.getAddress());*/
                /*if (value.get("userId").asInt() == 12344) {
                    return true;
                }*/
                return true;
            }
        }).keyBy(new KeySelector<KafkaBean, String>() {
            @Override
            public String getKey(KafkaBean value) throws Exception {
                return "key";
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(
                        new ProcessWindowFunction<KafkaBean, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<KafkaBean> elements, Collector<String> out) throws Exception {
                                StringBuffer sb = new StringBuffer();
                                sb.append("窗口范围是:").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window().getStart()))
                                        .append("----").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window().getEnd())).append("\n");

                                final Iterator<KafkaBean> iterator = elements.iterator();
                                while (iterator.hasNext()) {
                                    KafkaBean kafkaBean = iterator.next();
                                    sb.append("date:").append(kafkaBean.getDate()).append("\t")
                                            .append("cid：").append(kafkaBean.getCid()).append("\t")
                                            .append("ename：").append(kafkaBean.getEname()).append("\t")
                                            .append("date：").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(kafkaBean.getDate())).append("\n");
                                }
                                out.collect(sb.toString());
                            }
                        }
                ).print();

        env.execute("kafka consumer demo");

/*        @Override
        public void apply(String s, TimeWindow window, Iterable<KafkaBean> input, Collector<Object> out) throws Exception {
            String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getStart());
            String end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getEnd());
            out.collect("触发的窗口时间--[ "+start+"---"+end+" ]");
        }*/


/*        @Override
        public void process(String key, ProcessWindowFunction<StationLog, String, String, TimeWindow>.Context context,
        Iterable<StationLog> elements, Collector<String> out) throws Exception {
            StationLog maxLog = elements.iterator().next();

            StringBuffer sb = new StringBuffer();
            sb.append("窗口范围是:").append(context.window().getStart()).append("----").append(context.window().getEnd()).append("\n");;
            sb.append("基站ID：").append(maxLog.getStationID()).append("\t")
                    .append("呼叫时间：").append(maxLog.getCallTime()).append("\t")
                    .append("主叫号码：").append(maxLog.getFrom()).append("\t")
                    .append("被叫号码：")    .append(maxLog.getTo()).append("\t")
                    .append("通话时长：").append(maxLog.getDuration()).append("\n");
            out.collect(sb.toString());*/
    }
}
