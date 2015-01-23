package com.jivesoftware.os.rcvs.hbase098;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;

/**
 *
 */
public class HBaseRowColumnValueStoreProvider implements
    RowColumnValueStoreProvider<HBaseRowColumnValueStoreInitializer.HBase098RowColumnValueStoreConfig, Exception> {

    @Override
    public Class<HBaseRowColumnValueStoreInitializer.HBase098RowColumnValueStoreConfig> getConfigurationClass() {
        return HBaseRowColumnValueStoreInitializer.HBase098RowColumnValueStoreConfig.class;
    }

    @Override
    public Class<? extends RowColumnValueStoreInitializer<Exception>> getInitializerClass() {
        return HBaseRowColumnValueStoreInitializer.class;
    }
}
