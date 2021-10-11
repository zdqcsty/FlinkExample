package xuwei.tech.kafka;

import cn.binarywang.tools.generator.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Properties;

public class KafkaProducer {

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
        String topic = "ceshis";
        org.apache.kafka.clients.producer.Producer<String, String> procuder = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        for (int i = 1; i < 1000; i++) {
            Thread.sleep(1000);
            procuder.send(new ProducerRecord<String, String>(topic, createRecord().toString()));
            System.out.println(createRecord().toString());
        }
    }

    public static JSONObject createRecord() throws Exception {

        //身份证号码
        ChineseIDCardNumberGenerator cid = (ChineseIDCardNumberGenerator) ChineseIDCardNumberGenerator.getInstance();
        //中文姓名
        ChineseNameGenerator cname = ChineseNameGenerator.getInstance();
        //英文姓名
        EnglishNameGenerator ename = EnglishNameGenerator.getInstance();
        //手机号
        ChineseMobileNumberGenerator phoneNumber = ChineseMobileNumberGenerator.getInstance();
        //电子邮箱
        EmailAddressGenerator email = (EmailAddressGenerator) EmailAddressGenerator.getInstance();
        //居住地址
        ChineseAddressGenerator address = (ChineseAddressGenerator) ChineseAddressGenerator.getInstance();

        long date = System.currentTimeMillis();

        JSONObject order = new JSONObject();

        order.put("date", date);
        order.put("formatDate", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
        order.put("cid", cid.generate());
        order.put("cname", cname.generate());
        order.put("ename", ename.generate());
        order.put("phoneNumber", phoneNumber.generate());
        order.put("email", email.generate());
        order.put("address", address.generate());

        return order;
    }
}
