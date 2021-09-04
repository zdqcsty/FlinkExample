package xuwei.tech.sink.twophase;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class OracleSinkFunction extends RichSinkFunction<String> {

    private static Connection connection;

    /**
     * 建立数据库连接
     *
     * @param parameters Configuration
     * @throws Exception Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
        System.out.println("Configuration: " + parameters);
        connection = DruidConnectionUtils.getConnection();
        connection.setAutoCommit(false);
    }
/*
    static String jdbcUrl = "jdbc:mysql://10.130.7.201:3306/test_zgh?serverTimezone=Asia/Shanghai&useSSL=false&autoReconnect=true&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8";
    static String username = "root";
    static String password = "kerberostest";
*/

    /**
     * 提交数据变更
     *
     * @param sql     操作语句
     * @param context Context
     */
    @Override
    public void invoke(String sql, Context context) throws SQLException {
        try {
            if (!"".equals(sql)) {
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.executeUpdate();
                statement.close();
                connection.commit();
            }
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