package xuwei.tech.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaBeanDeserializat implements KafkaDeserializationSchema<KafkaBean> {

    @Override
    public boolean isEndOfStream(KafkaBean nextElement) {
        return false;
    }

    @Override
    public KafkaBean deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        System.out.println(new String(record.value(), "utf8"));
        return JSON.parseObject(new String(record.value(), "utf8"), KafkaBean.class);
    }

    @Override
    public TypeInformation<KafkaBean> getProducedType() {
        return null;
    }
}
