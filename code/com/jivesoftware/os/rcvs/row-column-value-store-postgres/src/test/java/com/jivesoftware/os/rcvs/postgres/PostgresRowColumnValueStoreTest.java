package com.jivesoftware.os.rcvs.postgres;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnTimestampRemove;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.TenantIdAndRow;
import com.jivesoftware.os.rcvs.api.TenantRowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.TenantRowColumnTimestampRemove;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.marshall.primatives.DoubleTypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.IntegerTypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.LongTypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.StringTypeMarshaller;
import com.jivesoftware.os.rcvs.postgres.PostgresRowColumnValueStoreInitializer.PostgresRowColumnValueStoreConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PostgresRowColumnValueStoreTest {

    private final String tableNameSpace = "rcvs_test";
    private final Random random = new Random();

    private PostgresRowColumnValueStoreInitializer initializer;

    @BeforeClass
    public void setUp() throws Exception {
        PostgresRowColumnValueStoreConfig config = BindInterfaceToConfiguration.bindDefault(PostgresRowColumnValueStoreConfig.class);
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/%s");
        config.setDriverClass("org.postgresql.Driver");
        config.setUsername("postgres");
        config.setPassword("postgres");

        initializer = new PostgresRowColumnValueStoreInitializer(config);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testAddGetRemove() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testAddGetRemove",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.add("1", 1L, 1, 1.0, null, null);
        Double value = rowColumnValueStore.get("1", 1L, 1, null, null);
        assertEquals(value, 1.0);
        rowColumnValueStore.remove("1", 1L, 1, null);
        value = rowColumnValueStore.get("1", 1L, 1, null, null);
        assertNull(value);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testAddIfNotExists() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testAddIfNotExists",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.add("1", 1L, 1, 1.0, null, null);
        boolean whether = rowColumnValueStore.addIfNotExists("1", 1L, 1, 2.0, null, null);
        assertFalse(whether);

        Double value = rowColumnValueStore.get("1", 1L, 1, null, null);
        assertEquals(value, 1.0);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testModify() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testModify",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.add("1", 1L, 1, 1.0, null, null);

        Double value = rowColumnValueStore.get("1", 1L, 1, null, null);
        assertEquals(value, 1.0);

        rowColumnValueStore.add("1", 1L, 1, 2.0, null, null);

        value = rowColumnValueStore.get("1", 1L, 1, null, null);
        assertEquals(value, 2.0);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testReplace() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testReplace",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.add("1", 1L, 1, 1.0, null, null);

        boolean whether = rowColumnValueStore.replaceIfEqualToExpected("1", 1L, 1, 2.0, 1.1, null, null);
        assertFalse(whether);

        Double value = rowColumnValueStore.get("1", 1L, 1, null, null);
        assertEquals(value, 1.0);

        whether = rowColumnValueStore.replaceIfEqualToExpected("1", 1L, 1, 3.0, 1.0, null, null);
        assertTrue(whether);

        value = rowColumnValueStore.get("1", 1L, 1, null, null);
        assertEquals(value, 3.0);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testMultiAddGetRemove() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testMultiAddGetRemove",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        Integer[] columnKeys = { 1, 11, 111, 1111 };
        Double[] columnValues = { 1.0, 11.0, 111.0, 1111.0 };

        rowColumnValueStore.multiAdd("1", 1L, columnKeys, columnValues, null, null);
        List<Double> values = rowColumnValueStore.multiGet("1", 1L, columnKeys, null, null);
        assertEquals(values.size(), columnValues.length);
        for (int i = 0; i < columnValues.length; i++) {
            assertEquals(values.get(i), columnValues[i]);
        }
        rowColumnValueStore.multiRemove("1", 1L, columnKeys, null);
        values = rowColumnValueStore.multiGet("1", 1L, columnKeys, null, null);
        assertEquals(values.size(), columnValues.length);
        for (int i = 0; i < columnValues.length; i++) {
            assertNull(values.get(i));
        }
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testMultiAddOverlap() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testMultiAddOverlap",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        Integer[] columnKeys = { 1, 11, 111, 1111 };
        Double[] columnValues = { 1.0, 11.0, 111.0, 1111.0 };

        rowColumnValueStore.multiAdd("1", 1L, columnKeys, columnValues, null, null);

        columnKeys = new Integer[] { 111, 1111, 11111, 111111 };
        columnValues = new Double[] { 222.0, 2222.0, 22222.0, 222222.0 };

        rowColumnValueStore.multiAdd("1", 1L, columnKeys, columnValues, null, null);

        List<Double> values = rowColumnValueStore.multiGet("1", 1L, new Integer[] { 1, 11, 111, 1111, 11111, 111111 }, null, null);
        assertEquals(values.size(), 6);
        assertEquals(values.get(0), 1.0);
        assertEquals(values.get(1), 11.0);
        assertEquals(values.get(2), 222.0);
        assertEquals(values.get(3), 2222.0);
        assertEquals(values.get(4), 22222.0);
        assertEquals(values.get(5), 222222.0);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testMultiRowsMultiAddGetRemove_1() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testMultiRowsMultiAddGetRemove",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.multiRowsMultiAdd("1", Arrays.asList(
            new RowColumValueTimestampAdd<>(-1L, -1, -1.0, null),
            new RowColumValueTimestampAdd<>(0L, 0, 0.0, null),
            new RowColumValueTimestampAdd<>(1L, 1, 1.0, null),
            new RowColumValueTimestampAdd<>(2L, 2, 2.0, null),
            new RowColumValueTimestampAdd<>(3L, 3, 3.0, null)));
        List<Map<Integer, Double>> values = rowColumnValueStore.multiRowMultiGet("1", Arrays.asList(0L, 1L, 2L, 4L), Arrays.asList(0, 1, 2, 4), null, null);

        assertEquals(values.size(), 4);
        assertEquals(values.get(0).get(0), 0.0); // 0L
        assertEquals(values.get(1).get(1), 1.0); // 1L
        assertEquals(values.get(2).get(2), 2.0); // 2L
        assertNull(values.get(3)); // 3L

        rowColumnValueStore.multiRowsMultiRemove("1", Arrays.asList(
            new RowColumnTimestampRemove<>(0L, 0, null),
            new RowColumnTimestampRemove<>(1L, 1, null),
            new RowColumnTimestampRemove<>(2L, 2, null)));
        values = rowColumnValueStore.multiRowMultiGet("1", Arrays.asList(-1L, 0L, 1L, 2L, 3L, 4L), Arrays.asList(-1, 0, 1, 2, 3, 4), null, null);

        assertEquals(values.size(), 6);
        assertEquals(values.get(0).get(-1), -1.0); // -1L
        assertNull(values.get(1)); // 0L
        assertNull(values.get(2)); // 1L
        assertNull(values.get(3)); // 2L
        assertEquals(values.get(4).get(3), 3.0); // 3L
        assertNull(values.get(5)); // 4L
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testMultiRowsMultiAddOverlap_1() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testMultiRowsMultiAddOverlap",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.multiRowsMultiAdd("1", Arrays.asList(
            new RowColumValueTimestampAdd<>(-1L, -1, -1.0, null),
            new RowColumValueTimestampAdd<>(0L, 0, 0.0, null),
            new RowColumValueTimestampAdd<>(1L, 1, 1.0, null),
            new RowColumValueTimestampAdd<>(2L, 2, 2.0, null)));

        rowColumnValueStore.multiRowsMultiAdd("1", Arrays.asList(
            new RowColumValueTimestampAdd<>(1L, 1, 1.1, null),
            new RowColumValueTimestampAdd<>(2L, 2, 2.2, null),
            new RowColumValueTimestampAdd<>(3L, 3, 3.3, null),
            new RowColumValueTimestampAdd<>(4L, 4, 4.4, null)));

        List<Map<Integer, Double>> values = rowColumnValueStore.multiRowMultiGet("1",
            Arrays.asList(-1L, 0L, 1L, 2L, 3L, 4L),
            Arrays.asList(-1, 0, 1, 2, 3, 4),
            null, null);

        assertEquals(values.size(), 6);
        assertEquals(values.get(0).get(-1), -1.0);
        assertEquals(values.get(1).get(0), 0.0);
        assertEquals(values.get(2).get(1), 1.1);
        assertEquals(values.get(3).get(2), 2.2);
        assertEquals(values.get(4).get(3), 3.3);
        assertEquals(values.get(5).get(4), 4.4);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testMultiRowsMultiAddGetRemove_2() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testMultiRowsMultiAddGetRemove",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.multiRowsMultiAdd(Arrays.asList(
            new TenantRowColumValueTimestampAdd<>("1", -1L, -1, -1.0, null),
            new TenantRowColumValueTimestampAdd<>("1", 0L, 0, 0.0, null),
            new TenantRowColumValueTimestampAdd<>("1", 1L, 1, 1.0, null),
            new TenantRowColumValueTimestampAdd<>("1", 2L, 2, 2.0, null),
            new TenantRowColumValueTimestampAdd<>("1", 3L, 3, 3.0, null)));
        List<Map<Integer, Double>> values = rowColumnValueStore.multiRowMultiGet("1", Arrays.asList(0L, 1L, 2L, 4L), Arrays.asList(0, 1, 2, 4), null, null);

        assertEquals(values.size(), 4);
        assertEquals(values.get(0).get(0), 0.0); // 0L
        assertEquals(values.get(1).get(1), 1.0); // 1L
        assertEquals(values.get(2).get(2), 2.0); // 2L
        assertNull(values.get(3)); // 3L

        rowColumnValueStore.multiRowsMultiRemove(Arrays.asList(
            new TenantRowColumnTimestampRemove<>("1", 0L, 0, null),
            new TenantRowColumnTimestampRemove<>("1", 1L, 1, null),
            new TenantRowColumnTimestampRemove<>("1", 2L, 2, null)));
        values = rowColumnValueStore.multiRowMultiGet("1", Arrays.asList(-1L, 0L, 1L, 2L, 3L, 4L), Arrays.asList(-1, 0, 1, 2, 3, 4), null, null);

        assertEquals(values.size(), 6);
        assertEquals(values.get(0).get(-1), -1.0); // -1L
        assertNull(values.get(1)); // 0L
        assertNull(values.get(2)); // 1L
        assertNull(values.get(3)); // 2L
        assertEquals(values.get(4).get(3), 3.0); // 3L
        assertNull(values.get(5)); // 4L
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testMultiRowsMultiAddOverlap_2() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testMultiRowsMultiAddOverlap",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.multiRowsMultiAdd(Arrays.asList(
            new TenantRowColumValueTimestampAdd<>("1", -1L, -1, -1.0, null),
            new TenantRowColumValueTimestampAdd<>("1", 0L, 0, 0.0, null),
            new TenantRowColumValueTimestampAdd<>("1", 1L, 1, 1.0, null),
            new TenantRowColumValueTimestampAdd<>("1", 2L, 2, 2.0, null)));

        rowColumnValueStore.multiRowsMultiAdd(Arrays.asList(
            new TenantRowColumValueTimestampAdd<>("1", 1L, 1, 1.1, null),
            new TenantRowColumValueTimestampAdd<>("1", 2L, 2, 2.2, null),
            new TenantRowColumValueTimestampAdd<>("1", 3L, 3, 3.3, null),
            new TenantRowColumValueTimestampAdd<>("1", 4L, 4, 4.4, null)));

        List<Map<Integer, Double>> values = rowColumnValueStore.multiRowMultiGet("1",
            Arrays.asList(-1L, 0L, 1L, 2L, 3L, 4L),
            Arrays.asList(-1, 0, 1, 2, 3, 4),
            null, null);

        assertEquals(values.size(), 6);
        assertEquals(values.get(0).get(-1), -1.0);
        assertEquals(values.get(1).get(0), 0.0);
        assertEquals(values.get(2).get(1), 1.1);
        assertEquals(values.get(3).get(2), 2.2);
        assertEquals(values.get(4).get(3), 3.3);
        assertEquals(values.get(5).get(4), 4.4);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testMultiGetEntriesWithoutKeys() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testMultiGetEntriesWithoutKeys",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        Integer[] columnKeys = { 1, 11, 111, 1111 };
        Double[] columnValues = { 1.0, 11.0, 111.0, 1111.0 };

        rowColumnValueStore.multiAdd("1", 1L, columnKeys, columnValues, null, null);
        ColumnValueAndTimestamp<Integer, Double, Long>[] entries = rowColumnValueStore.multiGetEntries("1", 1L, null, null, null);
        assertEquals(entries.length, columnValues.length);
        for (int i = 0; i < columnValues.length; i++) {
            assertEquals(entries[i].getColumn(), columnKeys[i]);
            assertEquals(entries[i].getValue(), columnValues[i]);
        }
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testGetKeysValuesEntries() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testGetKeysValuesEntries",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        Integer[] columnKeys = { -11, -1, 1, 11 };
        Double[] columnValues = { -11.0, -1.0, 1.0, 11.0 };

        rowColumnValueStore.multiAdd("1", 1L, columnKeys, columnValues, null, null);

        final List<Integer> actualKeys = Lists.newArrayList();
        rowColumnValueStore.getKeys("1", 1L, -1, 2L, 1_000, false, null, null, new CallbackStream<Integer>() {
            @Override
            public Integer callback(Integer key) throws Exception {
                if (key != null) {
                    actualKeys.add(key);
                }
                return key;
            }
        });
        assertEquals(actualKeys.size(), 2);
        assertEquals(actualKeys.get(0).intValue(), -1);
        assertEquals(actualKeys.get(1).intValue(), 1);

        final List<Double> actualValues = Lists.newArrayList();
        rowColumnValueStore.getValues("1", 1L, -1, 2L, 1_000, false, null, null, new CallbackStream<Double>() {
            @Override
            public Double callback(Double value) throws Exception {
                if (value != null) {
                    actualValues.add(value);
                }
                return value;
            }
        });
        assertEquals(actualValues.size(), 2);
        assertEquals(actualValues.get(0), -1.0);
        assertEquals(actualValues.get(1), 1.0);

        final List<ColumnValueAndTimestamp<Integer, Double, Long>> actualEntries = Lists.newArrayList();
        rowColumnValueStore.getEntrys("1", 1L, -1, 2L, 1_000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<Integer, Double, Long>>() {
                @Override
                public ColumnValueAndTimestamp<Integer, Double, Long> callback(ColumnValueAndTimestamp<Integer, Double, Long> entry) throws Exception {
                    if (entry != null) {
                        actualEntries.add(entry);
                    }
                    return entry;
                }
            });
        assertEquals(actualEntries.size(), 2);
        assertEquals(actualEntries.get(0).getColumn().intValue(), -1);
        assertEquals(actualEntries.get(0).getValue(), -1.0);
        assertEquals(actualEntries.get(1).getColumn().intValue(), 1);
        assertEquals(actualEntries.get(1).getValue(), 1.0);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testGetAllRowKeys() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testGetAllRowKeys",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.multiRowsMultiAdd("1", Arrays.asList(
            new RowColumValueTimestampAdd<>(-1L, -1, -1.0, null),
            new RowColumValueTimestampAdd<>(0L, 0, 0.0, null),
            new RowColumValueTimestampAdd<>(1L, 1, 1.0, null),
            new RowColumValueTimestampAdd<>(2L, 2, 2.0, null),
            new RowColumValueTimestampAdd<>(3L, 3, 3.0, null)));

        final List<Long> actualRows = Lists.newArrayList();
        rowColumnValueStore.getAllRowKeys(1_000, null, new CallbackStream<TenantIdAndRow<String, Long>>() {
            @Override
            public TenantIdAndRow<String, Long> callback(TenantIdAndRow<String, Long> tenantIdAndRow) throws Exception {
                if (tenantIdAndRow != null) {
                    assertEquals(tenantIdAndRow.getTenantId(), "1");
                    actualRows.add(tenantIdAndRow.getRow());
                }
                return tenantIdAndRow;
            }
        });
        assertEquals(actualRows.size(), 5);
        assertEquals(actualRows.get(0).longValue(), -1L);
        assertEquals(actualRows.get(1).longValue(), 0L);
        assertEquals(actualRows.get(2).longValue(), 1L);
        assertEquals(actualRows.get(3).longValue(), 2L);
        assertEquals(actualRows.get(4).longValue(), 3L);
    }

    @Test(enabled = false, description = "requires local postgres")
    public void testGetRowKeys() throws Exception {
        RowColumnValueStore<String, Long, Integer, Double, Exception> rowColumnValueStore = initializer.initialize(tableNameSpace,
            "testGetRowKeys",
            randomColumnFamily(),
            new DefaultRowColumnValueStoreMarshaller<>(new StringTypeMarshaller(),
                new LongTypeMarshaller(),
                new IntegerTypeMarshaller(),
                new DoubleTypeMarshaller()),
            new CurrentTimestamper());

        rowColumnValueStore.multiRowsMultiAdd("1", Arrays.asList(
            new RowColumValueTimestampAdd<>(-1L, -1, -1.0, null),
            new RowColumValueTimestampAdd<>(0L, 0, 0.0, null),
            new RowColumValueTimestampAdd<>(1L, 1, 1.0, null),
            new RowColumValueTimestampAdd<>(2L, 2, 2.0, null),
            new RowColumValueTimestampAdd<>(3L, 3, 3.0, null)));

        final List<Long> actualRows = Lists.newArrayList();
        rowColumnValueStore.getRowKeys("1", 0L, 3L, 1_000, null, new CallbackStream<TenantIdAndRow<String, Long>>() {
            @Override
            public TenantIdAndRow<String, Long> callback(TenantIdAndRow<String, Long> tenantIdAndRow) throws Exception {
                if (tenantIdAndRow != null) {
                    assertEquals(tenantIdAndRow.getTenantId(), "1");
                    actualRows.add(tenantIdAndRow.getRow());
                }
                return tenantIdAndRow;
            }
        });
        assertEquals(actualRows.size(), 3);
        assertEquals(actualRows.get(0).longValue(), 0L);
        assertEquals(actualRows.get(1).longValue(), 1L);
        assertEquals(actualRows.get(2).longValue(), 2L);

        actualRows.clear();
        rowColumnValueStore.getRowKeys("1", 1L, null, 1_000, null, new CallbackStream<TenantIdAndRow<String, Long>>() {
            @Override
            public TenantIdAndRow<String, Long> callback(TenantIdAndRow<String, Long> tenantIdAndRow) throws Exception {
                if (tenantIdAndRow != null) {
                    assertEquals(tenantIdAndRow.getTenantId(), "1");
                    actualRows.add(tenantIdAndRow.getRow());
                }
                return tenantIdAndRow;
            }
        });
        assertEquals(actualRows.size(), 3);
        assertEquals(actualRows.get(0).longValue(), 1L);
        assertEquals(actualRows.get(1).longValue(), 2L);
        assertEquals(actualRows.get(2).longValue(), 3L);

        actualRows.clear();
        rowColumnValueStore.getRowKeys("1", null, 2L, 1_000, null, new CallbackStream<TenantIdAndRow<String, Long>>() {
            @Override
            public TenantIdAndRow<String, Long> callback(TenantIdAndRow<String, Long> tenantIdAndRow) throws Exception {
                if (tenantIdAndRow != null) {
                    assertEquals(tenantIdAndRow.getTenantId(), "1");
                    actualRows.add(tenantIdAndRow.getRow());
                }
                return tenantIdAndRow;
            }
        });
        assertEquals(actualRows.size(), 3);
        assertEquals(actualRows.get(0).longValue(), -1L);
        assertEquals(actualRows.get(1).longValue(), 0L);
        assertEquals(actualRows.get(2).longValue(), 1L);
    }

    private String randomColumnFamily() {
        final String columnFamilyChars = "abcdefghijklmnopqrstuvwxyz1234567890";
        StringBuilder buf = new StringBuilder(8);
        for (int i = 0; i < 8; i++) {
            buf.append(columnFamilyChars.charAt(random.nextInt(columnFamilyChars.length())));
        }
        return buf.toString();
    }
}