package xuwei.tech.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Test {

    private PreparedStatement ps = null;
    private Connection connection = null;
    static String driver = "com.mysql.jdbc.Driver";
    static String jdbcUrl = "jdbc:mysql://10.130.7.201:3306/test_zgh?serverTimezone=Asia/Shanghai&useSSL=false&autoReconnect=true&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8";
    static String username = "root";
    static String password = "kerberostest";


    public static void main(String[] args) throws Exception {

        String sql="insert into test values  ('bbb')";

        Class.forName(driver);
        Connection connection = DriverManager.getConnection(jdbcUrl,username,password);
        connection.prepareStatement(sql).execute();
 /*       ResultSet resultSet = connection.prepareStatement(sql).executeQuery();

        while (resultSet.next()){
            final String string = resultSet.getString(1);
            System.out.println(string);
        }*/
    }
}
