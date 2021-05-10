package xuwei.tech.state;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

public class StateExample {
    static final SimpleDateFormat YYYY_MM_DD_HH = new SimpleDateFormat("yyyyMMdd HH");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
        //env.setStateBackend(new RocksDBStateBackend("oss://bigdata/xxx/order-state"));
        List<Order> data = new LinkedList<>();
        for (long i = 1; i <= 6; i++)
            data.add(new Order(i, i % 3, i % 3, i + 0.1));
        DataStream<Order> dataStream = env.fromCollection(data).setParallelism(1).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.milliseconds(1)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.finishTime;
            }
        });

        dataStream.keyBy(o -> o.memberId).map(
                new RichMapFunction<Order, List<Order>>() {
                    //mapstate是针对一个key来说的
                    MapState<Long, Order> mapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        MapStateDescriptor<Long, Order> productRank = new MapStateDescriptor<Long, Order>("productRank", Long.class, Order.class);
                        mapState = getRuntimeContext().getMapState(productRank);
                    }

                    @Override
                    public List<Order> map(Order value) throws Exception {
                        System.out.println(mapState.isEmpty());
                        if (mapState.contains(value.productId)) {
                            Order acc = mapState.get(value.productId);
                            value.sale += acc.sale;
                        }
                        mapState.put(value.productId, value);
                        return IteratorUtils.toList(mapState.values().iterator());
                    }
                }
        ).print();
        env.execute("example");
    }


    public static class Order {
        //finishTime: Long, memberId: Long, productId: Long, sale: Double
        public long finishTime;
        public long memberId;
        public long productId;
        public double sale;

        public Order() {
        }

        public Order(Long finishTime, Long memberId, Long productId, Double sale) {
            this.finishTime = finishTime;
            this.memberId = memberId;
            this.productId = productId;
            this.sale = sale;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "finishTime=" + finishTime +
                    ", memberId=" + memberId +
                    ", productId=" + productId +
                    ", sale=" + sale +
                    '}';
        }
    }
}