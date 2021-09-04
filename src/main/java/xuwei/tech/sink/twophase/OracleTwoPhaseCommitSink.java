package xuwei.tech.sink.twophase;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class OracleTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<String, OracleTwoPhaseCommitSink.ConnectionState, Void> {

    public OracleTwoPhaseCommitSink() {
        super(new KryoSerializer<>(ConnectionState.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     *
     * @return 数据库连接
     * @throws Exception exception
     */
    @Override
    protected ConnectionState beginTransaction() throws Exception {
        System.out.println("=====> beginTransaction... ");
        Connection connection = DruidConnectionUtils.getConnection();
        connection.setAutoCommit(false);
        return new ConnectionState(connection);
    }


    /**
     * 执行数据入库操作
     *
     * @param connection 连接
     * @param sql        执行SQL
     * @param context    context
     */
    @Override
    protected void invoke(ConnectionState connection, String sql, Context context) {
        System.out.println("++++++++++++invoke");
        try {
            System.out.println("11111");
            PreparedStatement statement = connection.connection.prepareStatement(sql);
            statement.execute();
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     *
     * @param connection ConnectionState
     */
    @Override
    protected void preCommit(ConnectionState connection) {
        System.out.println("hahahahha");
    }


    /**
     * 如果invoke执行正常则提交事物
     *
     * @param connection ConnectionState
     */
    @Override
    protected void commit(ConnectionState connection) {
        System.out.println("=====> commit... ");
        try {
            connection.connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("提交事物异常");
        }
    }


    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     *
     * @param connection Connection
     */
    @Override
    protected void abort(ConnectionState connection) {
        System.out.println("=====> abort... ");

        try {
            connection.connection.rollback();
        } catch (SQLException e) {
            throw new RuntimeException("回滚事物异常");

        }
    }

    //定义建立数据库连接的方法
    public static class ConnectionState {
        private final transient Connection connection;

        public ConnectionState(Connection connection) {
            this.connection = connection;
        }
    }
}
