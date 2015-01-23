package com.jivesoftware.os.rcvs.hbase098;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;

/**
 *
 */
public class HBaseRowColumnValueStoreProvider implements
    RowColumnValueStoreProvider<HBaseRowColumnValueStoreInitializer.HBase098RowColumnValueStoreConfig, HBaseRowColumnValueStoreInitializer, Exception> {

    @Override
    public Class<HBaseRowColumnValueStoreInitializer.HBase098RowColumnValueStoreConfig> getConfigurationClass() {
        return HBaseRowColumnValueStoreInitializer.HBase098RowColumnValueStoreConfig.class;
    }

    @Override
    public HBaseRowColumnValueStoreInitializer create(HBaseRowColumnValueStoreInitializer.HBase098RowColumnValueStoreConfig config) {
        return new HBaseRowColumnValueStoreInitializer(config);
    }
}
