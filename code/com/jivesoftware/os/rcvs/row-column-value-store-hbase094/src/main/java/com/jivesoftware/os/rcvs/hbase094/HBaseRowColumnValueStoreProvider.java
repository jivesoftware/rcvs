package com.jivesoftware.os.rcvs.hbase094;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;

/**
 *
 */
public class HBaseRowColumnValueStoreProvider implements
    RowColumnValueStoreProvider<HBaseRowColumnValueStoreInitializer.HBase094RowColumnValueStoreConfig, HBaseRowColumnValueStoreInitializer, Exception> {

    @Override
    public Class<HBaseRowColumnValueStoreInitializer.HBase094RowColumnValueStoreConfig> getConfigurationClass() {
        return HBaseRowColumnValueStoreInitializer.HBase094RowColumnValueStoreConfig.class;
    }

    @Override
    public HBaseRowColumnValueStoreInitializer create(HBaseRowColumnValueStoreInitializer.HBase094RowColumnValueStoreConfig config) {
        return new HBaseRowColumnValueStoreInitializer(config);
    }
}
