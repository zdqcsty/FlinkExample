package xuwei.tech.test.car;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class ProduceData {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.130.7.206:9092");
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "caranalyse";
        org.apache.kafka.clients.producer.Producer<String, String> procuder = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);

        Random random = new Random(1000000);

        for (int i = 1; i < 10000000; i++) {
            Thread.sleep(1);
            final int i1 = random.nextInt(1000000);

            String arr[] = {"biyadi", "tesila", "changcheng"};
            final int i2 = random.nextInt(2);

            procuder.send(new ProducerRecord<String, String>(topic, createRecord(i1, arr[i2]).toString()));
        }
    }

    public static JSONObject createRecord(int vin, String factory) throws Exception {

        JSONObject order = new JSONObject();

        order.put("vin", vin);
        order.put("factory", factory);

        return order;
    }


}
