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
package com.jivesoftware.os.rcvs.hbase;

import com.jivesoftware.os.rcvs.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.marshall.primatives.StringTypeMarshaller;
import com.jivesoftware.os.rcvs.tests.BaseRowColumnValueStore;
import com.jivesoftware.os.rcvs.tests.EmbeddedHBase;
import java.util.UUID;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
@Test (groups = "slow")
public class HBaseRowColumnValueStoreTest extends BaseRowColumnValueStore {

    private final EmbeddedHBase embeddedHBase = new EmbeddedHBase();

    @BeforeClass
    public void startHBase() throws Exception {
        embeddedHBase.start(true);
    }

    @BeforeMethod
    public void setUpMethod() throws Exception {
        String env = UUID.randomUUID().toString();
        HBaseSetOfSortedMapsImplInitializer.HBaseSetOfSortedMapsConfig hbaseConfig = BindInterfaceToConfiguration.bindDefault(
            HBaseSetOfSortedMapsImplInitializer.HBaseSetOfSortedMapsConfig.class);
        hbaseConfig.setMarshalThreadPoolSize(4);
        final SetOfSortedMapsImplInitializer<Exception> hBase = new HBaseSetOfSortedMapsImplInitializer(hbaseConfig, embeddedHBase.getConfiguration());
        RowColumnValueStore<String, String, String, String, Exception> store =
            hBase.initialize(env, "table", "columnFamily",
                new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                    new StringTypeMarshaller(), new StringTypeMarshaller(),
                    new StringTypeMarshaller()), new CurrentTimestamper());
        setStore(store);

    }

    @AfterClass
    public void stopHBase() throws Exception {
        embeddedHBase.stop();
    }

    @Test (groups = "slow")
    @Override
    public void testAdd() throws Exception {
        super.testAdd();
    }

    @Test (groups = "slow")
    @Override
    public void testGetEntries() throws Exception {
        super.testGetEntries();
    }

    @Test (groups = "slow")
    @Override
    public void testGetKeys() throws Exception {
        super.testGetKeys();
    }

    @Test (groups = "slow")
    @Override
    public void testGetValues() throws Exception {
        super.testGetValues();
    }

    @Test (groups = "slow")
    @Override
    public void testMultiAdd() throws Exception {
        super.testMultiAdd();
    }

    @Test (groups = "slow")
    @Override
    public void testMultiGet() throws Exception {
        super.testMultiGet();
    }

    @Test (groups = "slow")
    @Override
    public void testMultiRowMultiGet() throws Exception {
        super.testMultiRowMultiGet();
    }

    @Test (groups = "slow")
    @Override
    public void testMultiGetEntries() throws Exception {
        super.testMultiGetEntries();
    }

    @Test (groups = "slow")
    @Override
    public void testMultiRemove() throws Exception {
        super.testMultiRemove();
    }

    @Test (groups = "slow")
    @Override
    public void testMultiRowGetAll() throws Exception {
        super.testMultiRowGetAll();
    }

    @Test (groups = "slow")
    @Override
    public void testMultiRowsMultiAdd() throws Exception {
        super.testMultiRowsMultiAdd();
    }

    @Test (groups = "slow")
    @Override
    public void testMultiRowsMultiRemove() throws Exception {
        super.testMultiRowsMultiRemove();
    }

    @Test (groups = "slow")
    @Override
    public void testRemove() throws Exception {
        super.testRemove();
    }

    @Test (groups = "slow")
    @Override
    public void testRemoveRow() throws Exception {
        super.testRemoveRow();
    }

    @Test (groups = "slow")
    @Override
    public void testPermanentRemoveRow() throws Exception {
        super.testPermanentRemoveRow();
    }

    @Test (groups = "slow")
    @Override
    public void testCheckAndAdd() throws Exception {
        super.testCheckAndAdd();
    }
}
