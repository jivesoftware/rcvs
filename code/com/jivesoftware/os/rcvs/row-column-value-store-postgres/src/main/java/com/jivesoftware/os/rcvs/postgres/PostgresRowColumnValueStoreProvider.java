package com.jivesoftware.os.rcvs.postgres;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 */
public class PostgresRowColumnValueStoreProvider implements
    RowColumnValueStoreProvider<PostgresRowColumnValueStoreInitializer.PostgresRowColumnValueStoreConfig, PostgresRowColumnValueStoreInitializer, Exception> {

    @Override
    public Class<PostgresRowColumnValueStoreInitializer.PostgresRowColumnValueStoreConfig> getConfigurationClass() {
        return PostgresRowColumnValueStoreInitializer.PostgresRowColumnValueStoreConfig.class;
    }

    @Override
    public PostgresRowColumnValueStoreInitializer create(PostgresRowColumnValueStoreInitializer.PostgresRowColumnValueStoreConfig config) throws IOException {
        try {
            return new PostgresRowColumnValueStoreInitializer(config);
        } catch (PropertyVetoException | SQLException e) {
            throw new IOException(e);
        }
    }
}
