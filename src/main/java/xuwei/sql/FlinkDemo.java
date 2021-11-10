package xuwei.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkDemo {

    public static void main(String[] args) throws Exception {

        //注册planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        //使用fsTableEnv
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        //创建kafka源
        String kafkaSourceSql = "CREATE TABLE MyUserTable ( src string,rowtime string ) WITH ( 'connector.type' = 'kafka', 'connector.version' = '0.10', 'connector.topic' = 'zhouhe_es5', 'update-mode' = 'append', 'connector.properties.bootstrap.servers' = '10.202.4.120:39092', 'connector.properties.zookeeper.connect' = '10.203.79.239:2181', 'connector.properties.group.id' = 'demoaaa', 'connector.startup-mode' = 'latest-offset' , 'format.type' = 'json')";

        tableEnv.executeSql(kafkaSourceSql);

        Table table = tableEnv.sqlQuery("select src from MyUserTable limit 10" );

        //sql 的这里是Row  而不是 string或者其他类型
        tableEnv.toAppendStream(table, Row.class).print();

        env.execute("aaaa" );
    }
}
