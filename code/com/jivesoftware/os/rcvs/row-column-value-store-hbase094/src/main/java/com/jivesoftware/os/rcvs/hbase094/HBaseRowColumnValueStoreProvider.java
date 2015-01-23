package com.jivesoftware.os.rcvs.hbase094;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;

/**
 *
 */
public class HBaseRowColumnValueStoreProvider implements
    RowColumnValueStoreProvider<HBaseRowColumnValueStoreInitializer.HBase094RowColumnValueStoreConfig, Exception> {

    @Override
    public Class<HBaseRowColumnValueStoreInitializer.HBase094RowColumnValueStoreConfig> getConfigurationClass() {
        return HBaseRowColumnValueStoreInitializer.HBase094RowColumnValueStoreConfig.class;
    }

    @Override
    public Class<? extends RowColumnValueStoreInitializer<Exception>> getInitializerClass() {
        return HBaseRowColumnValueStoreInitializer.class;
    }
}
