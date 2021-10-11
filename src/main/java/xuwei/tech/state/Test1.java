/*
package xuwei.tech.state;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flink.config.flinkConstants;
import com.flink.model.DeviceAlarm;
import com.flink.model.DeviceData;
import com.flink.utils.emqtt.EmqttSource;
import com.flink.utils.mysql.JdbcReader;
import com.flink.utils.mysql.JdbcWriter;
import com.flink.utils.opentsdb.OpnetsdbWriter;
import com.flink.utils.redis.RedisWriter;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;


public class EmqttFlinkMain {
    private static Map<String, String> deviceMap = new Hashtable<String, String>();

    public static void main(String[] args) throws Exception {
        flinkConstants fc = flinkConstants.getInstance();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        */
/**
         *  从mysql获取machID， 五分钟刷新一次  本博客讲解的地方
         *//*

        DataStream<Map<String, String>> deviceStream = env.addSource(new JdbcReader());
        deviceStream.broadcast().map(new MapFunction<Map<String, String>, Object>() {
            @Override
            public Object map(Map<String, String> value) {
                deviceMap = value;
                return null;
            }
        });

        // ========================================================================


        //emqtt
        DataStream<Tuple2<String, String>> inputStream = env.addSource(new EmqttSource());

        */
/**
         *  数据类型
         *//*

        DataStream<DeviceData> dataStream = inputStream
                .rebalance()
                .flatMap(new FlatMapFunction<Tuple2<String, String>, DeviceData>() {
                    @Override
                    public void flatMap(Tuple2<String, String> value, Collector<DeviceData> out) {
                        String message = value.f0;
                        String topic = value.f1;
                        List<DeviceData> d = DataHandle(message, topic);
                        for (DeviceData line : d) {
                            out.collect(line);
                        }
                    }
                });


        //写入opentsdb
        dataStream.addSink(new OpnetsdbWriter()).name("opentsdb");

        //写入redis
        SingleOutputStreamOperator<Tuple2<String, String>> keyedStream = dataStream
                .map(new MapFunction<DeviceData, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(DeviceData value) {
                        String key = value.getCompID() + "/" + value.getMachID() + "/" + value.getOperationValue();
                        return Tuple2.of(key, value.getOperationData());
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                .process(new ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, String>> elements, Collector<Tuple2<String, String>> out) throws Exception {
                        Iterator<Tuple2<String, String>> iter = elements.iterator();
                        while (iter.hasNext()) {
                            Tuple2<String, String> temp = iter.next();
                            if (!iter.hasNext()) {
                                out.collect(temp);
                            }
                        }
                    }
                });
        keyedStream.addSink(new RedisWriter()).name("redis");

        */
/**
         *  告警类型
         *//*

        //写入mysql
        DataStream<List<DeviceAlarm>> alarmStream = inputStream.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                JSONObject AlarmObject = JSON.parseObject(value.f0);
                String dataType = (String) AlarmObject.get("type");
                return dataType.equals("Alarm") || dataType.equals("alarm");
            }
        }).map(new MapFunction<Tuple2<String, String>, List<DeviceAlarm>>() {
            @Override
            public List<DeviceAlarm> map(Tuple2<String, String> s) throws Exception {
                return alarmDnalysis(s.f0, s.f1);
            }
        });
        //调用JdbcWriter
        alarmStream.addSink(new JdbcWriter()).name("mysql").setParallelism(3);

        //调用JdbcWriterAsyncFunction
//        // create async function, which will *wait* for a while to simulate the process of async i/o
//        AsyncFunction<List<DeviceAlarm>, String> function = new JdbcWriterAsyncFunction();
//
//        // add async operator to streaming job
//        AsyncDataStream.orderedWait(
//                alarmStream,
//                function,
//                10000L,
//                TimeUnit.MILLISECONDS,
//                20).name("async write mysql").setParallelism(3);

        env.execute("EmqttFlinkMain");
    }

    private static List<DeviceData> DataHandle(String message, String topic) {
        List<DeviceData> d = new ArrayList<>();
        try {
            JSONObject DataObject = JSON.parseObject(message);
            String dataType = (String) DataObject.get("type");
            if (dataType.equals("Data") || dataType.equals("data")) {
                String[] array = topic.split("/");

                JSONArray dataList = JSON.parseArray(DataObject.get("values").toString());

                String machID = deviceMap.get(array[1]);
                if (machID != null) {
                    for (int i = 0; i < dataList.size(); i++) {
                        DeviceData d1 = new DeviceData();
                        JSONObject dataDict = dataList.getJSONObject(i);
                        d1.setMachID(machID);
                        d1.setCompID(array[0]);
                        d1.setGateMac(array[1]);
                        d1.setOperationValue(dataDict.get("name").toString());
                        d1.setOperationData(dataDict.get("data").toString());
                        d1.setGatherTime(dataDict.get("time").toString());
                        d.add(d1);
                    }
                } else {
                    System.out.println("无法解析数据");
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return d;
    }
}*/
