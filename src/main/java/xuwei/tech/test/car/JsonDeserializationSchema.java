package xuwei.tech.test.car;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class JsonDeserializationSchema extends AbstractDeserializationSchema<Car> {

    @Override
    public Car deserialize(byte[] message) throws IOException {
        try {
            String msg = new String(message, "utf-8");
            return JSONObject.parseObject(msg, Car.class);
        } catch (Exception e) {
            return null;
        }
    }
}