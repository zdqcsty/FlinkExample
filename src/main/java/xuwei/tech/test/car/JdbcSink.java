package xuwei.tech.test.car;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class JdbcSink extends RichSinkFunction<CarCounter> {

    private Connection connection = null;


    private final String JDBC_URL = "jdbc:mysql://10.130.7.201:3306/test_zgh?serverTimezone=Asia/Shanghai&useSSL=false&autoReconnect=true&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8";

    private final String USERNAME = "root";
    private final String PASSWORD = "kerberostest";


    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("Configuration: " + parameters);
        connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
    }

    @Override
    public void invoke(CarCounter counter, Context context) throws SQLException {
        String sql="REPLACE INTO car_analyse (factory,counter) VALUES(?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, counter.factory);
            statement.setInt(2, counter.counter);
            System.out.println("aaaaa---"+counter.counter);
            statement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
