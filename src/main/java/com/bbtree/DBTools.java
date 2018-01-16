package com.bbtree;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by chenzhilei on 16/5/17.
 * JDBC
 */
public class DBTools {
    private static final Logger logger = LoggerFactory.getLogger(DBTools.class);
    final static String JDBC_URL = "jdbc:mysql://10.162.98.164:3306/zhihuishu?user=reader&password=bbtree123123";
    final static String DRIVER = "com.mysql.jdbc.Driver";

    public static Connection getConn() {

        //        public static String JDBC_URL = "jdbc:mysql://114.215.178.73:3306/zhihuishu?user=reader&password=bbtree123123";
        Connection conn = null;
        try {
            Class.forName(DRIVER); //classLoader,加载对应驱动
            conn = DriverManager.getConnection(JDBC_URL);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static String getKeyCodeOfJDBC(String uuid) {
        Connection conn = DBTools.getConn();
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        String keyCode = "";
        try {
            assert conn != null;
            ps = conn.prepareStatement(String.format("SELECT key_code FROM zhihuishu.zhs_device WHERE uuid='%s'", uuid));
            resultSet = ps.executeQuery();
            if (resultSet.next()) {
                keyCode = resultSet.getString("key_code");

            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

            try {
                assert resultSet != null;
                resultSet.close();
                ps.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (StringUtils.isBlank(keyCode)) {
            logger.debug("keycode is not exists,please check mysql db !!!");
        }
        return keyCode;
    }

}
