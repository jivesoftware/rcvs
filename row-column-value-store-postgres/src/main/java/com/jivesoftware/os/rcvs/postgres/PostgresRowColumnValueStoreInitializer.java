/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.rcvs.postgres;

import com.google.common.collect.Maps;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

public class PostgresRowColumnValueStoreInitializer implements RowColumnValueStoreInitializer<Exception> {

    static public interface PostgresRowColumnValueStoreConfig extends Config {

        @StringDefault("unspecifiedJdbcUrl")
        String getJdbcUrl();

        void setJdbcUrl(String jdbcUrl);

        @StringDefault("unspecifiedDriverClass")
        String getDriverClass();

        void setDriverClass(String driveClass);

        @IntDefault(24)
        int getMarshalThreadPoolSize();

        void setMarshalThreadPoolSize(int marshalThreadPoolSize);

        @StringDefault("unspecifiedUsername")
        String getUsername();

        void setUsername(String username);

        @StringDefault("unspecifiedPassword")
        String getPassword();

        void setPassword(String password);

        @IntDefault(2)
        int getMinPoolSize();

        @IntDefault(20)
        int getMaxPoolSize();

        @IntDefault(3_600_000)
        int getMaxConnectionAgeSeconds();

        @IntDefault(10)
        int getJdbcConnectAttempts();

        @IntDefault(5_000)
        int getJdbcConnectRetryDelay();

        @IntDefault(60_000)
        int getJdbcLoginTimeoutInSeconds();
    }

    private final PostgresRowColumnValueStoreConfig config;
    private final Map<String, PooledDataSource> dataSources;
    private final ExecutorService marshalExecutor;

    public PostgresRowColumnValueStoreInitializer(PostgresRowColumnValueStoreConfig config) throws PropertyVetoException, SQLException {
        this.config = config;
        this.dataSources = Maps.newHashMap();
        this.marshalExecutor = Executors.newFixedThreadPool(config.getMarshalThreadPoolSize());
    }

    private PooledDataSource getDataSource(String tableNameSpace) throws PropertyVetoException, SQLException {

        synchronized (dataSources) {
            PooledDataSource existing = dataSources.get(tableNameSpace);
            if (existing == null) {
                ComboPooledDataSource dataSource = new ComboPooledDataSource();
                dataSource.setDriverClass(config.getDriverClass());

                dataSource.setAutoCommitOnClose(true);
                dataSource.setUser(config.getUsername());
                dataSource.setPassword(config.getPassword());
                dataSource.setJdbcUrl(config.getJdbcUrl().replace("%s", tableNameSpace));
                dataSource.setMinPoolSize(config.getMinPoolSize());
                dataSource.setMaxPoolSize(config.getMaxPoolSize());
                dataSource.setMaxConnectionAge(config.getMaxConnectionAgeSeconds());

                dataSource.setAcquireRetryAttempts(config.getJdbcConnectAttempts());
                dataSource.setAcquireRetryDelay(config.getJdbcConnectRetryDelay());
                dataSource.setLoginTimeout(config.getJdbcLoginTimeoutInSeconds());

                existing = dataSource;
                dataSources.put(tableNameSpace, dataSource);
            }

            return existing;
        }
    }

    private void checkTable(PooledDataSource dataSource, String tableName, String columnFamilyName) throws SQLException {
        String table = tableName + "_" + columnFamilyName;
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + table +
            " (" +
            " row bytea not null," +
            " col bytea not null," +
            " val bytea not null," +
            " ts bigint not null," +
            " PRIMARY KEY (row, col)" +
            " )";
        String createIndexSql = "CREATE INDEX row_col_index ON " + table + " (row, col)";
        try (Connection con = dataSource.getConnection()) {
            PreparedStatement pstmt = con.prepareStatement(createTableSql);
            int count = pstmt.executeUpdate();
            pstmt.close();

            if (count > 0) {
                pstmt = con.prepareStatement(createIndexSql);
                pstmt.executeUpdate();
                pstmt.close();
            }
        }
    }

    private String normalize(String nameSpace) {
        return nameSpace.replace('.', '_');
    }

    @Override
    public <T, R, C, V> RowColumnValueStore<T, R, C, V, Exception> initialize(String tableNameSpace,
        String tableName,
        String columnFamilyName,
        RowColumnValueStoreMarshaller<T, R, C, V> marshaller,
        Timestamper timestamper) throws IOException {

        tableNameSpace = normalize(tableNameSpace);
        tableName = normalize(tableName);
        columnFamilyName = normalize(columnFamilyName);

        try {
            PooledDataSource dataSource = getDataSource(tableNameSpace);
            checkTable(dataSource, tableName, columnFamilyName);
            return new PostgresRowColumnValueStore<>(
                dataSource,
                tableName,
                columnFamilyName,
                marshaller,
                timestamper,
                marshalExecutor);
        } catch (PropertyVetoException | SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public <T, R, C, V> RowColumnValueStore<T, R, C, V, Exception> initialize(
        String tableNameSpace,
        String tableName,
        String columnFamilyName,
        String[] additionalColumnFamilies,
        RowColumnValueStoreMarshaller<T, R, C, V> marshaller,
        Timestamper timestamper) throws IOException {

        tableNameSpace = normalize(tableNameSpace);
        tableName = normalize(tableName);
        columnFamilyName = normalize(columnFamilyName);

        try {
            PooledDataSource dataSource = getDataSource(tableNameSpace);
            checkTable(dataSource, tableName, columnFamilyName);
            return new PostgresRowColumnValueStore<>(
                dataSource,
                tableName,
                columnFamilyName,
                marshaller,
                timestamper,
                marshalExecutor);
        } catch (PropertyVetoException | SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public <T, R, C, V> RowColumnValueStore<T, R, C, V, Exception> initialize(
        String tableNameSpace,
        String tableName,
        String columnFamilyName,
        int ttlInSeconds,
        int minVersions,
        int maxVersions,
        RowColumnValueStoreMarshaller<T, R, C, V> marshaller,
        Timestamper timestamper) throws IOException {

        tableNameSpace = normalize(tableNameSpace);
        tableName = normalize(tableName);
        columnFamilyName = normalize(columnFamilyName);

        try {
            PooledDataSource dataSource = getDataSource(tableNameSpace);
            checkTable(dataSource, tableName, columnFamilyName);
            return new PostgresRowColumnValueStore<>(
                dataSource,
                tableName,
                columnFamilyName,
                marshaller,
                timestamper,
                marshalExecutor);
        } catch (PropertyVetoException | SQLException e) {
            throw new IOException(e);
        }
    }
}
