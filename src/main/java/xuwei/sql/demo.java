package xuwei.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class demo {

    public static void main(String[] args) throws Exception {


        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv,bsSettings);

        DataStream<Tuple2<String, Integer>> socketStream = fsEnv.socketTextStream("10.202.145.249", 10008)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        Table table = fsTableEnv.fromDataStream(socketStream, "word,number");

        Table table1 = fsTableEnv.sqlQuery("select word,number from " + table);


        fsTableEnv.toRetractStream()


        fsEnv.execute("aaaa");
    }
}
