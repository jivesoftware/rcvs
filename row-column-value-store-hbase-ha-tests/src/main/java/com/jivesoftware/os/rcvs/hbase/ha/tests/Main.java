package com.jivesoftware.os.rcvs.hbase.ha.tests;

import com.jivesoftware.os.rcvs.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.MasterSlaveHARowColumnValueStore;
import com.jivesoftware.os.rcvs.api.MasterSlaveHARowColumnValueStore.ReadFailureMode;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.hbase098.HBaseRowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.hbase098.HBaseRowColumnValueStoreInitializer.HBase098RowColumnValueStoreConfig;
import com.jivesoftware.os.rcvs.marshall.primatives.LongTypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.StringTypeMarshaller;
import java.io.IOException;
import org.merlin.config.BindInterfaceToConfiguration;

public class Main {

    public static void main(String[] args) throws Exception {
        HBase098RowColumnValueStoreConfig masterConfig = BindInterfaceToConfiguration.bindDefault(HBase098RowColumnValueStoreConfig.class);
        masterConfig.setHBaseZookeeperQuorum(args[0]);
        HBase098RowColumnValueStoreConfig slaveConfig = BindInterfaceToConfiguration.bindDefault(HBase098RowColumnValueStoreConfig.class);
        slaveConfig.setHBaseZookeeperQuorum(args[1]);

        final MasterSlaveHARowColumnValueStore<String, String, String, Long, Exception> store = new MasterSlaveHARowColumnValueStore<>(
                getStore(masterConfig),
                ReadFailureMode.failToSlave,
                getStore(slaveConfig));

        final String tenantId = "ha-test";
        final String rowKey = "ha";
        final String columnKey = "test";

        final long sleep = 10;
        Thread reader = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Long timestamp = store.get(tenantId, rowKey, columnKey, null, null);
                        if (timestamp != null) {
                            long lag = System.currentTimeMillis() - timestamp;
                            if (lag >= 0) {
                                System.out.println("INFO: Lag:" + lag);
                            } else {
                                System.out.println("WARN: applied change has disappeared. Lag:" + lag);
                            }
                        } else {
                            System.out.println("INFO: No timestamp in store.");
                        }
                        Thread.sleep(sleep);
                    } catch (Exception x) {
                        System.out.println("ERROR: Reader barfed." + x);
                    }
                }
            }
        };

        Thread writer = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        store.add(tenantId, rowKey, columnKey, System.currentTimeMillis(), null, null);
                        Thread.sleep(sleep);
                    } catch (Exception x) {
                        System.out.println("ERROR: Writer barfed." + x);
                    }
                }
            }
        };

        writer.start();
        reader.start();

    }

    private static RowColumnValueStore<String, String, String, Long, Exception> getStore(HBase098RowColumnValueStoreConfig config) throws IOException {
        HBaseRowColumnValueStoreInitializer masterRowColumnValueStoreInitializer = new HBaseRowColumnValueStoreInitializer(config);
        return masterRowColumnValueStoreInitializer.initialize(
                "ha", "ha.test", "cf", new DefaultRowColumnValueStoreMarshaller<>(
                new StringTypeMarshaller(),
                new StringTypeMarshaller(),
                new StringTypeMarshaller(),
                new LongTypeMarshaller()), new CurrentTimestamper());
    }
}
