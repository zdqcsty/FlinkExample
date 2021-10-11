package xuwei.tech.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import xuwei.tech.sink.twophase.DruidConnectionUtils;
import xuwei.tech.test.car.CarCounter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlSinkFunction extends RichSinkFunction<CarCounter> {

    private static Connection connection;

    /**
     * 建立数据库连接
     *
     * @param parameters Configuration
     * @throws Exception Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("Configuration: " + parameters);
        connection = DruidConnectionUtils.getConnection();
    }

    @Override
    public void invoke(CarCounter carCounter, Context context) throws SQLException {
        try {

            PreparedStatement statement = connection.prepareStatement("UPDATE car_analyse SET counter=? WHERE factory=?");
            statement.executeUpdate();
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            connection.rollback();
        }
    }

    /**
     * 关闭数据库连接
     *
     * @throws Exception Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }
}