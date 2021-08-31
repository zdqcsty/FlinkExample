package xuwei.tech.state;

import com.mysql.jdbc.Driver;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


/**
 * RichSourceFunction  是单线程的富源    
 */
public class JdbcReader extends RichSourceFunction<List<String>> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;


    private final String JDBC_URL = "jdbc:mysql://10.130.7.201:3306/bcloud_test001?serverTimezone=Asia/Shanghai&useSSL=false&autoReconnect=true&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8";

    private final String USERNAME = "root";
    private final String PASSWORD = "kerberostest";

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        DriverManager.registerDriver(new Driver());
        connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);//获取连接
        ps = connection.prepareStatement("select * from test_zgh.test");
    }

    //执行查询并获取结果
    @Override
    public void run(SourceContext<List<String>> ctx) throws Exception {

        List<String> list = new ArrayList<>();

        try {
            while (isRunning) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String name = resultSet.getString("name");
                    list.add(name);
                }
                ctx.collect(list);//发送结果
                list.clear();
                Thread.sleep(20000);
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }

    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
        isRunning = false;
    }
}