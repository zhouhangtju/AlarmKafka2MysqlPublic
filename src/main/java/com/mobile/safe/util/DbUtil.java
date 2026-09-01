package com.mobile.safe.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
@Component
public class DbUtil {

    @Autowired
    private Environment env;

    public void addPartition(String table, String partition, String ds) {
        // 执行DDL语句创建分区
        String ddl = String.format("alter table %s ADD PARTITION(PARTITION %s values in ('%s'))", table, partition, ds);
        log.info("===sql: {}",ddl);
        dbHandle(ddl);
    }

    public void createPartition(String table, String partition, String ds) {
        // 执行DDL语句创建分区
        //String ddl = String.format("alter table %s ADD PARTITION(PARTITION %s values in ('%s'))", table, partition, ds);
        String ddl = String.format("alter table %s PARTITION BY LIST COLUMNS(ds)(PARTITION %s values in ('%s'))", table, partition, ds);
        log.info("===sql: {}",ddl);
        dbHandle(ddl);
    }

    public void delPartition(String table, String ds) {
        // 执行DDL语句创建分区
        //String ddl = String.format("alter table %s DROP PARTITION if exists %s;", table, ds);
        String ddl = String.format("alter table %s DROP PARTITION %s;", table, ds);
        log.info("===sql: {}",ddl);
        dbHandle(ddl);
    }

    public void dbHandle(String sql) {
        // JDBC连接信息
        String jdbcUrl = env.getProperty("spring.datasource.druid.url");
        String username = env.getProperty("spring.datasource.druid.username");
        String password = env.getProperty("spring.datasource.druid.password");
        log.info("===db info:{},{},{}", jdbcUrl, username,password);
        Connection connection = null;
        Statement statement = null;

        try {
            // 建立数据库连接
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            // 创建Statement对象
            statement = connection.createStatement();
            // 执行DDL语句创建分区
            statement.execute(sql);
            log.info("sql操作成功: {}", sql);
        } catch (SQLException e) {
            log.error("sql execute error", e);
        } finally {
            // 关闭Statement和Connection
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                log.error("close connection error", e);
            }
        }
    }
}
