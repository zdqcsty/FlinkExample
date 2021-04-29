package xuwei.tech.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class MysqlSink extends RichSinkFunction<String> {

    private Statement statement = null;
    private Connection connection = null;
    String driver = "com.mysql.jdbc.Driver";
    String jdbcUrl = "jdbc:mysql://10.130.7.201:3306/test_zgh?serverTimezone=Asia/Shanghai&useSSL=false&autoReconnect=true&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8";
    String username = "root";
    String password = "kerberostest";

    private Connection getConn() throws Exception {
        Class.forName(driver);
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println("aaaaaaaaaaaaaaaa");
        statement.execute("insert into test values  ('" + value + "')");
    }

    //初始化方法
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbb");
        connection = getConn();
        statement = connection.createStatement();
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("cccccccccccccccccccccccc");
        if (statement != null) {
            statement.close();
        }
        if (statement != null) {
            connection.close();
        }
    }
}
