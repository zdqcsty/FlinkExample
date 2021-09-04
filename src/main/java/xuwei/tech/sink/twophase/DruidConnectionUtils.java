package xuwei.tech.sink.twophase;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DruidConnectionUtils {

    private transient static DataSource dataSource = null;
    private final transient static Properties PROPERTIES = new Properties();


    // 静态代码块
    static {
        PROPERTIES.put("driverClassName", "com.mysql.jdbc.Driver");
        PROPERTIES.put("url", "jdbc:mysql://10.130.7.201:3306/test_zgh?serverTimezone=Asia/Shanghai&useSSL=false&autoReconnect=true&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8");
        PROPERTIES.put("username", "root");
        PROPERTIES.put("password", "kerberostest");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(PROPERTIES);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DruidConnectionUtils() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}