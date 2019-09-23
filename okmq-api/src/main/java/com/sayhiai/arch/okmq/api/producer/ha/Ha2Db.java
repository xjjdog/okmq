package com.sayhiai.arch.okmq.api.producer.ha;

import com.alibaba.druid.pool.DruidDataSource;
import com.sayhiai.arch.okmq.api.Packet;
import com.sayhiai.arch.okmq.api.producer.AbstractProducer;
import com.sayhiai.arch.okmq.api.producer.HaException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class Ha2Db implements HA {

    private DBConnection dbConnection;

    private final static String Props_Prefix = "okmq.h2.";

    private final static int DEFAULT_LENGTH = 32768;

    private static final String INIT_DB = "CREATE TABLE okmq_msg (id VARCHAR(255) PRIMARY KEY NOT NULL,topic VARCHAR(255)  NOT NULL,content VARCHAR(%s) NOT NULL,createtime bigint(20) NOT NULL);CREATE INDEX IDX_OKMQ_CREATETIME ON okmq_msg(createtime);";

    private static final String SQL_INSERT = "INSERT INTO okmq_msg VALUES('%s','%s','%s',%s)";

    private static final String SQL_DELETE = "DELETE FROM okmq_msg WHERE id = '%s'";

    private static final String SQL_QUERY = "SELECT TOP 1000 id,topic,content,createtime FROM okmq_msg ORDER BY createtime ASC";

    @Override
    public void close() {
        dbConnection.close();
        org.h2.Driver.unload();
    }



    @Override
    public void configure(Properties properties) {
        dbConnection = new DBConnection();

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(DBConnection.class);
            for (PropertyDescriptor desc : beanInfo.getPropertyDescriptors()) {
                final String name = desc.getName();
                final Object value = properties.get(Props_Prefix + name);
                if (null != value && !"".equals(value)) {
                    Method method = desc.getWriteMethod();
                    method.invoke(dbConnection, properties.get(Props_Prefix + name));
                }
            }
        } catch (Exception e) {
            log.error("error while config h2 . pls check it !!");
        }

        org.h2.Driver.load();
        Connection conn = dbConnection.connect();
        try (Statement stat = conn.createStatement()) {
            stat.execute(String.format(INIT_DB, dbConnection.getDataLength()>0?dbConnection.getDataLength():DEFAULT_LENGTH));
        } catch (SQLException e) {
            log.warn("h2 table already exist");
        }

    }

    @Override
    public void preSend(Packet packet) throws HaException {

        this.savePacket(packet);
    }

    @Override
    public void postSend(Packet packet) throws HaException {

        this.deletePacket(packet.getIdentify());
    }

    @Override
    public void doRecovery(AbstractProducer producer) throws HaException {
        try (Statement stat = dbConnection.getConnection().createStatement()) {
            ResultSet rs = stat.executeQuery(SQL_QUERY);
            List<Packet> result = convertPacket(rs);
            while (result.size()>0){
                for (Packet packet: result) {
                    producer.sendAsync(packet);
                    deletePacket(packet.getIdentify());
                }
                rs = stat.executeQuery(SQL_QUERY);
                result = convertPacket(rs);
            }
        } catch (Exception e) {
            throw new HaException("okmq:h2:" + e.getMessage());
        }

    }

    private void savePacket(Packet packet) throws HaException {
        String sql = String.format(SQL_INSERT, packet.getIdentify(), packet.getTopic(), packet.getContent(), packet.getTimestamp());
        try (Statement stat = dbConnection.getConnection().createStatement()) {
            stat.execute(sql);
        }catch(JdbcSQLIntegrityConstraintViolationException e){
            log.error("do recovery message ,please check kafka status and local network!!!");
        }catch (SQLException e) {
            throw new HaException("okmq:h2:" + e.getMessage());
        }
    }

    private void deletePacket(String id) throws HaException {
        String sql = String.format(SQL_DELETE, id);
        try (Statement stat = dbConnection.getConnection().createStatement()) {
            stat.execute(sql);
        } catch (SQLException e) {
            throw new HaException("okmq:h2:" + e.getMessage());
        }
    }

    private List<Packet> convertPacket(ResultSet rs) throws SQLException {
        List<Packet> result = new ArrayList<>(1000);
        Packet packet;
        while (rs.next()) {
            packet = new Packet();
            packet.setIdentify(rs.getString(1));
            packet.setTopic(rs.getString(2));
            packet.setContent(rs.getString(3));
            packet.setTimestamp(rs.getLong(4));
            result.add(packet);
        }
        return result;
    }

    public class DBConnection {

        @Getter
        @Setter
        private String url;
        @Getter
        @Setter
        private String user;
        @Getter
        @Setter
        private String passwd;
        @Getter
        @Setter
        private int dataLength;
        private String DRIVER_CLASS = "org.h2.Driver";

        private Connection connection;

        private DruidDataSource dataSource;

        public DBConnection() {
        }

        public DBConnection(String url, String user, String passwd) {
            this.url = url;
            this.user = user;
            this.passwd = passwd;
        }

        public Connection connect() {

            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setDriverClassName(DRIVER_CLASS);
            dataSource.setUrl(this.url);
            dataSource.setUsername(this.user);
            dataSource.setPassword(this.passwd);
            dataSource.setMaxActive(10);
            dataSource.setInitialSize(2);
            dataSource.setMaxWait(60000);
            dataSource.setMinIdle(2);
            dataSource.setTimeBetweenEvictionRunsMillis(60000);
            dataSource.setMinEvictableIdleTimeMillis(300000);
            dataSource.setValidationQuery("select 1");
            dataSource.setTestWhileIdle(false);
            dataSource.setTestOnBorrow(false);
            dataSource.setTestOnReturn(false);
            dataSource.setRemoveAbandoned(false);
            dataSource.setRemoveAbandonedTimeout(1800);
            dataSource.setLogAbandoned(true);
            dataSource.setPoolPreparedStatements(true);
            dataSource.setMaxOpenPreparedStatements(30);

            try {
                connection = dataSource.getConnection();
                this.dataSource = dataSource;
                return this.connection;
            } catch (SQLException e) {
                log.error("h2 connection error", e);
            }
            return null;
        }

        public Connection getConnection() {
            if(this.connection == null){
                try {
                    this.connection = this.dataSource.getConnection();
                }catch (SQLException e) {
                    log.error("h2 connection error", e);
                }
            }
            return this.connection;
        }

        public void close() {
            try {
                this.connection.close();
            } catch (SQLException e) {
                log.error("close h2 connection error", e);
            }
        }

    }


}
