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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.CallbackStreamException;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.KeyedColumnValueCallbackStream;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnTimestampRemove;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreMarshallerException;
import com.jivesoftware.os.rcvs.api.TenantIdAndRow;
import com.jivesoftware.os.rcvs.api.TenantKeyedColumnValueCallbackStream;
import com.jivesoftware.os.rcvs.api.TenantRowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.TenantRowColumnTimestampRemove;
import com.jivesoftware.os.rcvs.api.ValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import com.jivesoftware.os.rcvs.shared.RowColumnValueStoreCounters;
import com.mchange.v2.c3p0.PooledDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * Postgres implementation of RowColumnValueStore generic interface.
 *
 * @param <T> type of object that will be used for tenantId information
 * @param <R> type of object that will be used for the row key
 * @param <C> type of object that will be used for column key
 * @param <V> type of object that will be used for values
 * @author jonathan
 */
public class PostgresRowColumnValueStore<T, R, C, V> implements RowColumnValueStore<T, R, C, V, Exception> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger(true);
    private final Timestamper timestamper;
    private final PooledDataSource dataSource;
    private final RowColumnValueStoreMarshaller<T, R, C, V> marshaller;
    private final String table;
    private final RowColumnValueStoreCounters counters;
    private final ExecutorService marshalExecutor;

    private final String insertSql;
    private final String updateSql;
    private final String updateIfExpectedSql;
    private final String selectValForRowColSql;
    private final String selectColValForRowInSql;
    private final String selectColValForRowAllSql;
    private final String selectColValForRowAllAscSql;
    private final String selectColValForRowAllDescSql;
    private final String selectColValForRowStartAscSql;
    private final String selectColValForRowStartDescSql;
    private final String selectRowAllSql;
    private final String selectRowRangeAscSql;
    private final String selectRowStartAscSql;
    private final String selectRowStopAscSql;
    private final String selectRowValForRowInColSql;
    private final String selectRowColValForRowInAscSql;
    private final String selectRowColValForRowInColInSql;
    private final String deleteColForRowExactSql;
    private final String deleteColForRowInSql;
    private final String deleteRowSql;

    /**
     * Create a new wrapper over an HBase table.
     *
     * @param dataSource      cannot be null
     * @param tableName       cannot be null
     * @param family          cannot be null
     * @param marshaller      cannot be null
     * @param timestamper     can't be null
     * @param marshalExecutor
     * @throws IOException
     */
    public PostgresRowColumnValueStore(
        PooledDataSource dataSource,
        String tableName,
        String family,
        RowColumnValueStoreMarshaller<T, R, C, V> marshaller,
        Timestamper timestamper,
        ExecutorService marshalExecutor) throws IOException {

        if (timestamper == null) {
            throw new IllegalArgumentException("timestamper cannot be null");
        }
        this.timestamper = timestamper;
        this.dataSource = dataSource;
        this.marshaller = marshaller;

        this.table = tableName + "_" + family;
        this.counters = new RowColumnValueStoreCounters(tableName);

        this.marshalExecutor = marshalExecutor;

        // initialize sql constants
        this.insertSql = "INSERT INTO " + table + " (row, col, val, ts) SELECT ?, ?, ?, ?" +
            " WHERE NOT EXISTS (SELECT 1 FROM " + table + " WHERE row = ? AND col = ?)";
        this.updateSql = "UPDATE " + table + " SET val = ?, ts = ? WHERE row = ? AND col = ? AND ts < ?";
        this.updateIfExpectedSql = "UPDATE " + table + " SET val = ?, ts = ? WHERE row = ? AND col = ? AND val = ? AND ts < ?";
        this.selectValForRowColSql = "SELECT val, ts FROM " + table + " WHERE row = ? AND col = ?";
        this.selectColValForRowAllSql = "SELECT col, val, ts FROM " + table + " WHERE row = ? ORDER BY col ASC";
        this.selectColValForRowInSql = "SELECT col, val, ts FROM " + table + " WHERE row = ? AND col IN (%a)";
        this.selectColValForRowAllAscSql = "SELECT col, val, ts FROM " + table + " WHERE row = ? ORDER BY col ASC";
        this.selectColValForRowAllDescSql = "SELECT col, val, ts FROM " + table + " WHERE row = ? ORDER BY col DESC";
        this.selectColValForRowStartAscSql = "SELECT col, val, ts FROM " + table + " WHERE row = ? AND col >= ? ORDER BY col ASC";
        this.selectColValForRowStartDescSql = "SELECT col, val, ts FROM " + table + " WHERE row = ? AND col >= ? ORDER BY col DESC";
        this.selectRowAllSql = "SELECT row FROM " + table + " ORDER BY row ASC";
        this.selectRowRangeAscSql = "SELECT row FROM " + table + " WHERE row >= ? AND row < ? ORDER BY row ASC";
        this.selectRowStartAscSql = "SELECT row FROM " + table + " WHERE row >= ? ORDER BY row ASC";
        this.selectRowStopAscSql = "SELECT row FROM " + table + " WHERE row < ? ORDER BY row ASC";
        this.selectRowValForRowInColSql = "SELECT row, val, ts FROM " + table + " WHERE row IN (%a) AND col = ?";
        this.selectRowColValForRowInAscSql = "SELECT row, col, val, ts FROM " + table + " WHERE row IN (%a) ORDER BY row ASC, col ASC";
        this.selectRowColValForRowInColInSql = "SELECT row, col, val, ts FROM " + table + " WHERE row IN (%a) AND col IN (%a)";
        this.deleteColForRowExactSql = "DELETE FROM " + table + " WHERE row = ? AND col = ? AND ts <= ?";
        this.deleteColForRowInSql = "DELETE FROM " + table + " WHERE row = ? AND col IN (%a) AND ts <= ?";
        this.deleteRowSql = "DELETE FROM " + table + " WHERE row = ? AND ts <= ?";
    }

    // Un-tested

    /**
     * @param rowKey
     * @param columnKeys
     * @param columnValues
     * @param timeToLiveInSeconds ignored.
     */
    @Override
    public void multiAdd(final T tenantId, final R rowKey, final C[] columnKeys, final V[] columnValues,
        final Integer timeToLiveInSeconds, Timestamper overrideTimestamper) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);
            final long timestamp = (overrideTimestamper == null) ? timestamper.get() : overrideTimestamper.get();
            final byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);

            PreparedStatement pstmt = con.prepareStatement(insertSql);
            for (int i = 0; i < columnKeys.length; i++) {
                byte[] rawColumnKey = marshaller.toColumnKeyBytes(columnKeys[i]);
                byte[] rawColumnValue = marshaller.toValueBytes(columnValues[i]);
                pstmt.setBytes(1, rawRowKey);
                pstmt.setBytes(2, rawColumnKey);
                pstmt.setBytes(3, rawColumnValue);
                pstmt.setLong(4, timestamp);
                pstmt.setBytes(5, rawRowKey);
                pstmt.setBytes(6, rawColumnKey);
                pstmt.addBatch();
            }
            int[] counts = pstmt.executeBatch();
            pstmt.close();

            pstmt = con.prepareStatement(updateSql);
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    continue;
                }

                byte[] rawColumnKey = marshaller.toColumnKeyBytes(columnKeys[i]);
                byte[] rawColumnValue = marshaller.toValueBytes(columnValues[i]);
                pstmt.setBytes(1, rawColumnValue);
                pstmt.setLong(2, timestamp);
                pstmt.setBytes(3, rawRowKey);
                pstmt.setBytes(4, rawColumnKey);
                pstmt.setLong(5, timestamp);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.close();
            con.commit();
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Exception multiAdd to postgres. customer=" + tenantId + " key=" + rowKey + " columnNames="
                + columnKeys + " columnValues=" + columnValues, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    @Override
    public void multiRowsMultiAdd(final T tenantId, List<RowColumValueTimestampAdd<R, C, V>> multiAdd) throws Exception {
        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);
            PreparedStatement pstmt = con.prepareStatement(insertSql);
            for (RowColumValueTimestampAdd<R, C, V> add : multiAdd) {
                byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, add.getRowKey());
                byte[] rawColumnKey = marshaller.toColumnKeyBytes(add.getColumnKey());
                byte[] rawColumnValue = marshaller.toValueBytes(add.getColumnValue());
                long timestamp = timestamper.get();
                if (add.getOverrideTimestamper() != null) {
                    timestamp = add.getOverrideTimestamper().get();
                }

                pstmt.setBytes(1, rawRowKey);
                pstmt.setBytes(2, rawColumnKey);
                pstmt.setBytes(3, rawColumnValue);
                pstmt.setLong(4, timestamp);
                pstmt.setBytes(5, rawRowKey);
                pstmt.setBytes(6, rawColumnKey);
                pstmt.addBatch();
            }
            int[] counts = pstmt.executeBatch();
            pstmt.close();

            pstmt = con.prepareStatement(updateSql);
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    continue;
                }

                RowColumValueTimestampAdd<R, C, V> add = multiAdd.get(i);
                byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, add.getRowKey());
                byte[] rawColumnKey = marshaller.toColumnKeyBytes(add.getColumnKey());
                byte[] rawColumnValue = marshaller.toValueBytes(add.getColumnValue());
                long timestamp = timestamper.get();
                if (add.getOverrideTimestamper() != null) {
                    timestamp = add.getOverrideTimestamper().get();
                }

                pstmt.setBytes(1, rawColumnValue);
                pstmt.setLong(2, timestamp);
                pstmt.setBytes(3, rawRowKey);
                pstmt.setBytes(4, rawColumnKey);
                pstmt.setLong(5, timestamp);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.close();
            con.commit();
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Exception multiAdd to postgres. customer=" + tenantId + " multiAdd=" + multiAdd, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    @Override
    public void multiRowsMultiAdd(List<TenantRowColumValueTimestampAdd<T, R, C, V>> multiAdd) throws Exception {
        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);
            PreparedStatement pstmt = con.prepareStatement(insertSql);
            for (TenantRowColumValueTimestampAdd<T, R, C, V> add : multiAdd) {
                byte[] rawRowKey = marshaller.toRowKeyBytes(add.getTenantId(), add.getRowKey());
                byte[] rawColumnKey = marshaller.toColumnKeyBytes(add.getColumnKey());
                byte[] rawColumnValue = marshaller.toValueBytes(add.getColumnValue());
                long timestamp = timestamper.get();
                if (add.getOverrideTimestamper() != null) {
                    timestamp = add.getOverrideTimestamper().get();
                }

                pstmt.setBytes(1, rawRowKey);
                pstmt.setBytes(2, rawColumnKey);
                pstmt.setBytes(3, rawColumnValue);
                pstmt.setLong(4, timestamp);
                pstmt.setBytes(5, rawRowKey);
                pstmt.setBytes(6, rawColumnKey);
                pstmt.addBatch();
            }
            int[] counts = pstmt.executeBatch();
            pstmt.close();

            pstmt = con.prepareStatement(updateSql);
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    continue;
                }

                TenantRowColumValueTimestampAdd<T, R, C, V> add = multiAdd.get(i);
                byte[] rawRowKey = marshaller.toRowKeyBytes(add.getTenantId(), add.getRowKey());
                byte[] rawColumnKey = marshaller.toColumnKeyBytes(add.getColumnKey());
                byte[] rawColumnValue = marshaller.toValueBytes(add.getColumnValue());
                long timestamp = timestamper.get();
                if (add.getOverrideTimestamper() != null) {
                    timestamp = add.getOverrideTimestamper().get();
                }

                pstmt.setBytes(1, rawColumnValue);
                pstmt.setLong(2, timestamp);
                pstmt.setBytes(3, rawRowKey);
                pstmt.setBytes(4, rawColumnKey);
                pstmt.setLong(5, timestamp);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.close();
            con.commit();
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Exception multiAdd to postgres. multiAdd=" + multiAdd, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    /**
     * @param rowKey
     * @param columnKey
     * @param columnValue
     * @param timeToLiveInSeconds ignored
     */
    @Override
    public void add(final T tenantId, final R rowKey, final C columnKey, final V columnValue,
        final Integer timeToLiveInSeconds, Timestamper overrideTimestamper) throws Exception {
        add(tenantId, rowKey, columnKey, columnValue, null, overrideTimestamper, false);
    }

    @Override
    public boolean addIfNotExists(final T tenantId, final R rowKey, final C columnKey, final V columnValue,
        final Integer timeToLiveInSeconds, final Timestamper overrideTimestamper) throws Exception {
        return add(tenantId, rowKey, columnKey, columnValue, null, overrideTimestamper, true);
    }

    @Override
    public boolean replaceIfEqualToExpected(T tenantId, R rowKey, C columnKey, V columnValue, V expectedValue,
        Integer timeToLiveInSeconds, Timestamper overrideTimestamper) throws Exception {
        return add(tenantId, rowKey, columnKey, columnValue, expectedValue, overrideTimestamper, true);
    }

    private boolean add(final T tenantId, final R rowKey, final C columnKey, final V columnValue, final V expectedValue,
        Timestamper overrideTimestamper, boolean checkValue) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            final long timestamp = (overrideTimestamper == null) ? timestamper.get() : overrideTimestamper.get();
            final byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);
            final byte[] rawColumnKey = marshaller.toColumnKeyBytes(columnKey);
            final byte[] rawColumnValue = marshaller.toValueBytes(columnValue);
            PreparedStatement pstmt;

            int inserted = 0;
            if (!checkValue || expectedValue == null) {
                pstmt = con.prepareStatement(insertSql);
                pstmt.setBytes(1, rawRowKey);
                pstmt.setBytes(2, rawColumnKey);
                pstmt.setBytes(3, rawColumnValue);
                pstmt.setLong(4, timestamp);
                pstmt.setBytes(5, rawRowKey);
                pstmt.setBytes(6, rawColumnKey);
                pstmt.addBatch();
                inserted = pstmt.executeUpdate();
                pstmt.close();
            }

            if (inserted > 0) {
                con.commit();
                return true;
            } else {
                if (checkValue) {
                    if (expectedValue == null) {
                        // needed to pass insertion case
                        con.commit();
                        return false;
                    } else {
                        final byte[] rawExpectedValue = marshaller.toValueBytes(expectedValue);
                        pstmt = con.prepareStatement(updateIfExpectedSql);
                        pstmt.setBytes(1, rawColumnValue);
                        pstmt.setLong(2, timestamp);
                        pstmt.setBytes(3, rawRowKey);
                        pstmt.setBytes(4, rawColumnKey);
                        pstmt.setBytes(5, rawExpectedValue);
                        pstmt.setLong(6, timestamp);
                        int updated = pstmt.executeUpdate();
                        pstmt.close();
                        con.commit();
                        return updated > 0;
                    }
                } else {
                    pstmt = con.prepareStatement(updateSql);
                    pstmt.setBytes(1, rawColumnValue);
                    pstmt.setLong(2, timestamp);
                    pstmt.setBytes(3, rawRowKey);
                    pstmt.setBytes(4, rawColumnKey);
                    pstmt.setLong(5, timestamp);
                    int updated = pstmt.executeUpdate();
                    pstmt.close();
                    con.commit();
                    return updated > 0;
                }
            }
        } catch (RowColumnValueStoreMarshallerException ex) {
            // slows down sending from another thread
            String columnValueAsString = columnValue.toString();
            if (columnValue instanceof byte[]) {
                columnValueAsString = toString((byte[]) columnValue, ".");
            }
            LOG.error("Exception writing to postgres. customer=" + tenantId + " key=" + rowKey + " columName=" + columnKey + " columnValue="
                + columnValueAsString, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    String toString(byte[] strings, String delim) {
        StringBuilder string = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            string.append(String.valueOf(strings[i]));
            if (i < strings.length - 1) {
                string.append(delim);
            }
        }
        return string.toString();
    }

    /**
     * Gets multiple columns from a row.
     *
     * @param tenantId                cannot be null
     * @param rowKey                  cannot be null
     * @param columnKeys
     * @param overrideNumberOfRetries
     * @param overrideConsistency
     * @return
     */
    @Override
    public List<V> multiGet(T tenantId, R rowKey, C[] columnKeys, Integer overrideNumberOfRetries, Integer overrideConsistency) throws Exception {
        List<V> got = new LinkedList<>();
        ColumnValueAndTimestamp<C, V, Long>[] entries = multiGetEntries(tenantId, rowKey, columnKeys, overrideNumberOfRetries, overrideConsistency);
        if (entries == null) {
            return null;
        }
        for (ColumnValueAndTimestamp<C, V, Long> entry : entries) {
            if (entry == null) {
                got.add(null);
            } else {
                got.add(entry.getValue());
            }
        }
        return got;
    }

    @Override
    public ColumnValueAndTimestamp<C, V, Long>[] multiGetEntries(T tenantId, R rowKey, C[] columnKeys, Integer overrideNumberOfRetries,
        Integer overrideConsistency) throws Exception {

        if (columnKeys != null) {
            return multiGetEntriesWithKeys(tenantId, rowKey, columnKeys);
        } else {
            return multiGetEntriesWithoutKeys(tenantId, rowKey);
        }
    }

    private ColumnValueAndTimestamp<C, V, Long>[] multiGetEntriesWithKeys(T tenantId, R rowKey, C[] columnKeys) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);
            byte[][] rawColumnKeys = new byte[columnKeys.length][];
            for (int i = 0; i < columnKeys.length; i++) {
                rawColumnKeys[i] = marshaller.toColumnKeyBytes(columnKeys[i]);
            }

            PreparedStatement pstmt = con.prepareStatement(applyInBytes(selectColValForRowInSql, rawColumnKeys));
            pstmt.setFetchSize(columnKeys.length); //TODO configure!
            pstmt.setBytes(1, rawRowKey);
            ResultSet rs = pstmt.executeQuery();

            Map<C, ColumnValueAndTimestamp<C, V, Long>> resultMap = Maps.newHashMap();
            while (rs.next()) {
                C c = marshaller.fromColumnKeyBytes(rs.getBytes(1));
                V v = marshaller.fromValueBytes(rs.getBytes(2));
                long timestamp = rs.getLong(3);
                resultMap.put(c, new ColumnValueAndTimestamp<>(c, v, timestamp));
            }

            @SuppressWarnings("unchecked")
            ColumnValueAndTimestamp<C, V, Long>[] got = new ColumnValueAndTimestamp[columnKeys.length];
            for (int i = 0; i < got.length; i++) {
                got[i] = resultMap.get(columnKeys[i]);
            }

            counters.got(1);
            return got;
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Failed to retrieve keys. customer=" + tenantId + " key=" + rowKey + " columnName=" + columnKeys, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    private ColumnValueAndTimestamp<C, V, Long>[] multiGetEntriesWithoutKeys(T tenantId, R rowKey) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);

            PreparedStatement pstmt = con.prepareStatement(selectColValForRowAllSql);
            pstmt.setFetchSize(100); //TODO configure!
            pstmt.setBytes(1, rawRowKey);
            ResultSet rs = pstmt.executeQuery();

            List<ColumnValueAndTimestamp<C, V, Long>> got = Lists.newArrayList();
            while (rs.next()) {
                C c = marshaller.fromColumnKeyBytes(rs.getBytes(1));
                V v = marshaller.fromValueBytes(rs.getBytes(2));
                long timestamp = rs.getLong(3);
                got.add(new ColumnValueAndTimestamp<>(c, v, timestamp));
            }
            counters.got(1);
            return got.toArray(new ColumnValueAndTimestamp[got.size()]);
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Failed to retrieve keys. customer=" + tenantId + " key=" + rowKey + " columnName=null", ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    /**
     * Gets a single column from a row.
     *
     * @param rowKey
     * @param columnKey
     * @param overRideConsistency ignored
     * @return
     * @throws Exception
     */
    @Override
    public V get(final T tenantId, final R rowKey, final C columnKey, Integer overrideNumberOfRetries, Integer overRideConsistency) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            final byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);
            final byte[] rawColumnKey = marshaller.toColumnKeyBytes(columnKey);

            PreparedStatement pstmt = con.prepareStatement(selectValForRowColSql);
            pstmt.setBytes(1, rawRowKey);
            pstmt.setBytes(2, rawColumnKey);
            ResultSet rs = pstmt.executeQuery();
            V v = null;
            if (rs.next()) {
                counters.got(1);
                v = marshaller.fromValueBytes(rs.getBytes(1));
            }
            rs.close();
            pstmt.close();
            return v;
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Failed to retrieve key. customer=" + tenantId + " key=" + rowKey + " columnName=" + columnKey, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    @Override
    public void multiRemove(final T tenantId, final R rowKey, final C[] columnKeys, Timestamper overrideTimestamper) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);
            byte[][] rawColumnKeys = new byte[columnKeys.length][];

            for (int i = 0; i < columnKeys.length; i++) {
                rawColumnKeys[i] = marshaller.toColumnKeyBytes(columnKeys[i]);
            }

            long timestamp = timestamper.get();
            if (overrideTimestamper != null) {
                timestamp = overrideTimestamper.get();
            }

            PreparedStatement pstmt = con.prepareStatement(applyInBytes(deleteColForRowInSql, rawColumnKeys));
            pstmt.setBytes(1, rawRowKey);
            pstmt.setLong(2, timestamp);
            pstmt.executeUpdate();
            pstmt.close();
            con.commit();
            counters.removed(columnKeys.length);
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Exception multiRemove from postgres. customer=" + tenantId + " key=" + rowKey + " columnNames=" + columnKeys, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    @Override
    public void multiRowsMultiRemove(final T tenantId, List<RowColumnTimestampRemove<R, C>> multiRemove) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            PreparedStatement pstmt = con.prepareStatement(deleteColForRowExactSql);
            for (RowColumnTimestampRemove<R, C> remove : multiRemove) {
                byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, remove.getRowKey());
                byte[] rawColumnKey = marshaller.toColumnKeyBytes(remove.getColumnKey());

                long timestamp = timestamper.get();
                if (remove.getOverrideTimestamper() != null) {
                    timestamp = remove.getOverrideTimestamper().get();
                }

                pstmt.setBytes(1, rawRowKey);
                pstmt.setBytes(2, rawColumnKey);
                pstmt.setLong(3, timestamp);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.close();
            con.commit();
            counters.removed(multiRemove.size());
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Exception multiAdd to postgres. customer=" + tenantId + " multiRemove=" + multiRemove, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    @Override
    public void multiRowsMultiRemove(List<TenantRowColumnTimestampRemove<T, R, C>> multiRemove) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            PreparedStatement pstmt = con.prepareStatement(deleteColForRowExactSql);
            for (TenantRowColumnTimestampRemove<T, R, C> remove : multiRemove) {
                byte[] rawRowKey = marshaller.toRowKeyBytes(remove.getTenantId(), remove.getRowKey());
                byte[] rawColumnKey = marshaller.toColumnKeyBytes(remove.getColumnKey());

                long timestamp = timestamper.get();
                if (remove.getOverrideTimestamper() != null) {
                    timestamp = remove.getOverrideTimestamper().get();
                }

                pstmt.setBytes(1, rawRowKey);
                pstmt.setBytes(2, rawColumnKey);
                pstmt.setLong(3, timestamp);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.close();
            con.commit();
            counters.removed(multiRemove.size());
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Exception multiAdd to postgres. multiRemove=" + multiRemove, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    /**
     * @param rowKey
     * @param columnKey
     * @throws Exception
     */
    @Override
    public void remove(final T tenantId, final R rowKey, final C columnKey, Timestamper overrideTimestamper) throws Exception {
        if (columnKey == null) {
            return;
        }

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            final long timestamp = (overrideTimestamper == null) ? timestamper.get() : overrideTimestamper.get();
            final byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);
            final byte[] rawColumnKey = marshaller.toColumnKeyBytes(columnKey);

            PreparedStatement pstmt = con.prepareStatement(deleteColForRowExactSql);
            pstmt.setBytes(1, rawRowKey);
            pstmt.setBytes(2, rawColumnKey);
            pstmt.setLong(3, timestamp);
            pstmt.executeUpdate();
            pstmt.close();
            con.commit();
            counters.removed(1);
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Failed to remove. customer=" + tenantId + " key=" + rowKey + " columnName=" + columnKey, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    /**
     * @param tenantId
     * @param rowKey
     * @param startColumnKey
     * @param maxCount                if null read entire column
     * @param batchSize
     * @param reversed
     * @param overrideNumberOfRetries ignored
     * @param overrideConsistency     ignored
     * @param callback
     */
    @Override
    public void getKeys(final T tenantId, R rowKey, C startColumnKey, Long maxCount, int batchSize, boolean reversed, Integer overrideNumberOfRetries,
        Integer overrideConsistency, CallbackStream<C> callback) throws Exception {
        get(tenantId, rowKey, startColumnKey, maxCount, batchSize, reversed, callback,
            new ValueStoreMarshaller<ColumnValueAndTimestamp<byte[], byte[], Long>, C>() {
                @Override
                public C marshall(ColumnValueAndTimestamp<byte[], byte[], Long> columnValueAndTimestamp) throws Exception {
                    return marshaller.fromColumnKeyBytes(columnValueAndTimestamp.getColumn());
                }
            });
    }

    /**
     * @param tenantId
     * @param rowKey
     * @param startColumnKey
     * @param maxCount                if null read entire column
     * @param batchSize
     * @param reversed
     * @param overrideNumberOfRetries ignored
     * @param overrideConsistency     ignored
     * @param callback
     */
    @Override
    public void getValues(final T tenantId, R rowKey, C startColumnKey, Long maxCount, int batchSize, boolean reversed, Integer overrideNumberOfRetries,
        Integer overrideConsistency, CallbackStream<V> callback) throws Exception {
        get(tenantId, rowKey, startColumnKey, maxCount, batchSize, reversed, callback,
            new ValueStoreMarshaller<ColumnValueAndTimestamp<byte[], byte[], Long>, V>() {
                @Override
                public V marshall(ColumnValueAndTimestamp<byte[], byte[], Long> columnValueAndTimestamp) throws Exception {
                    return marshaller.fromValueBytes(columnValueAndTimestamp.getValue());
                }
            });
    }

    /**
     * @param <TS>
     * @param tenantId
     * @param rowKey
     * @param startColumnKey
     * @param maxCount                if null read entire column
     * @param batchSize
     * @param reversed
     * @param overrideNumberOfRetries ignored
     * @param overrideConsistency     ignored
     * @param callback
     */
    @Override
    public <TS> void getEntrys(final T tenantId, R rowKey, C startColumnKey, Long maxCount, int batchSize, boolean reversed,
        Integer overrideNumberOfRetries, Integer overrideConsistency, CallbackStream<ColumnValueAndTimestamp<C, V, TS>> callback) throws Exception {

        get(tenantId, rowKey, startColumnKey, maxCount, batchSize, reversed, callback,
            new ValueStoreMarshaller<ColumnValueAndTimestamp<byte[], byte[], Long>, ColumnValueAndTimestamp<C, V, TS>>() {
                @Override
                public ColumnValueAndTimestamp<C, V, TS> marshall(ColumnValueAndTimestamp<byte[], byte[], Long> columnValueAndTimestamp) throws Exception {
                    return new ColumnValueAndTimestamp<>(
                        marshaller.fromColumnKeyBytes(columnValueAndTimestamp.getColumn()),
                        marshaller.fromValueBytes(columnValueAndTimestamp.getValue()),
                        (TS) columnValueAndTimestamp.getTimestamp());
                }
            });
    }

    /**
     * @param tenantId
     * @param rowKey
     * @param startColumnKey
     * @param maxCount
     * @param batchSize
     * @param reversed
     * @param callback
     * @param marshall
     * @param <K>
     */
    private <K> void get(final T tenantId,
        final R rowKey,
        final C startColumnKey,
        final Long maxCount,
        int batchSize,
        final boolean reversed,
        final CallbackStream<K> callback,
        final ValueStoreMarshaller<ColumnValueAndTimestamp<byte[], byte[], Long>, K> marshall)
        throws Exception {

        final MutableLong gotCount = new MutableLong();
        final int desiredBatchSize = (maxCount == null) ? batchSize : (int) Math.min(maxCount, batchSize);
        final int marshalBatchSize = 24; //TODO expose to config

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            final byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);

            PreparedStatement pstmt;
            if (startColumnKey == null) {
                pstmt = con.prepareStatement(applyLimit(reversed ? selectColValForRowAllDescSql : selectColValForRowAllAscSql, maxCount));
                pstmt.setBytes(1, rawRowKey);
            } else {
                byte[] startColumnKeyBytes = marshaller.toColumnKeyBytes(startColumnKey);
                pstmt = con.prepareStatement(applyLimit(reversed ? selectColValForRowStartDescSql : selectColValForRowStartAscSql, maxCount));
                pstmt.setBytes(1, rawRowKey);
                pstmt.setBytes(2, startColumnKeyBytes);
            }

            pstmt.setFetchSize(batchSize);
            ResultSet rs = pstmt.executeQuery();

            List<Future<K>> marshalFutures = Lists.newArrayListWithCapacity(marshalBatchSize);
            EOS:
            while (rs.next()) {
                final ColumnValueAndTimestamp<byte[], byte[], Long> columnValueAndTimestamp = new ColumnValueAndTimestamp<>(
                    rs.getBytes(1), rs.getBytes(2), rs.getLong(3));
                counters.sliced(1);

                marshalFutures.add(marshalExecutor.submit(new Callable<K>() {
                    @Override
                    public K call() throws Exception {
                        return marshall.marshall(columnValueAndTimestamp);
                    }
                }));

                if (marshalFutures.size() == marshalBatchSize) {
                    if (completeFutureCallbacks(maxCount, callback, gotCount, marshalFutures)) {
                        marshalFutures.clear();
                        break EOS;
                    } else {
                        marshalFutures.clear();
                    }
                }
            }
            rs.close();
            pstmt.close();

            if (!marshalFutures.isEmpty()) {
                completeFutureCallbacks(maxCount, callback, gotCount, marshalFutures);
            }

            // EOS end of stream
            try {
                callback.callback(null);
            } catch (Exception ex) {
                throw new CallbackStreamException(ex);
            }
        } catch (Exception ex) {
            LOG.error("Failed to get slice. customer=" + tenantId + " key=" + rowKey + " start=" + startColumnKey, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    private <K> boolean completeFutureCallbacks(Long maxCount, CallbackStream<K> callback, MutableLong gotCount, List<Future<K>> marshalFutures)
        throws InterruptedException, ExecutionException {

        for (Future<K> future : marshalFutures) {
            K marshalled = future.get();
            if (marshalled == null) {
                continue;
            }

            try {
                K returned = callback.callback(marshalled);
                if (marshalled != returned) {
                    return true;
                }
                gotCount.increment();
                if (maxCount != null) {
                    if (gotCount.longValue() >= maxCount) {
                        return true;
                    }
                }
            } catch (Exception ex) {
                throw new CallbackStreamException(ex);
            }
        }
        return false;
    }

    /**
     * @param batchSize
     * @param overrideNumberOfRetries ignored
     * @param callback
     */
    @Override
    public void getAllRowKeys(int batchSize, Integer overrideNumberOfRetries, CallbackStream<TenantIdAndRow<T, R>> callback) throws Exception {
        getRowKeys(null, null, null, batchSize, overrideNumberOfRetries, callback);
    }

    /**
     * @param tenantId
     * @param startRowKey
     * @param stopRowKey
     * @param batchSize
     * @param overrideNumberOfRetries
     * @param callback
     * @throws Exception
     */
    @Override
    public void getRowKeys(T tenantId, R startRowKey, R stopRowKey, int batchSize, Integer overrideNumberOfRetries,
        CallbackStream<TenantIdAndRow<T, R>> callback) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            PreparedStatement pstmt = null;
            if (tenantId != null) {
                if (startRowKey != null && stopRowKey != null) {
                    byte[] startRowKeyBytes = marshaller.toRowKeyBytes(tenantId, startRowKey);
                    byte[] stopRowKeyBytes = marshaller.toRowKeyBytes(tenantId, stopRowKey);
                    pstmt = con.prepareStatement(selectRowRangeAscSql);
                    pstmt.setBytes(1, startRowKeyBytes);
                    pstmt.setBytes(2, stopRowKeyBytes);
                } else if (startRowKey != null) {
                    pstmt = con.prepareStatement(selectRowStartAscSql);
                    byte[] startRowKeyBytes = marshaller.toRowKeyBytes(tenantId, startRowKey);
                    pstmt.setBytes(1, startRowKeyBytes);
                } else if (stopRowKey != null) {
                    pstmt = con.prepareStatement(selectRowStopAscSql);
                    byte[] stopRowKeyBytes = marshaller.toRowKeyBytes(tenantId, stopRowKey);
                    pstmt.setBytes(1, stopRowKeyBytes);
                }
            }
            if (pstmt == null) {
                pstmt = con.prepareStatement(selectRowAllSql);
            }

            pstmt.setFetchSize(batchSize);
            ResultSet rs = pstmt.executeQuery();
            EOS:
            while (rs.next()) {
                try {
                    byte[] rawRowKey = rs.getBytes(1);
                    if (rawRowKey == null) {
                        continue;
                    }
                    TenantIdAndRow<T, R> entry = marshaller.fromRowKeyBytes(rawRowKey);
                    try {
                        if (callback.callback(entry) != entry) {
                            // stop stream requested
                            break;
                        }
                    } catch (Exception ex) {
                        throw new CallbackStreamException(ex);
                    }
                } catch (Exception x) {
                    LOG.error("unable to handle keySlice.", x);
                }
            }
            rs.close();
            pstmt.close();

            // EOS end of stream
            try {
                callback.callback(null);
            } catch (Exception ex) {
                throw new CallbackStreamException(ex);
            }

        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    //TODO needs tests
    @Override
    public void removeRow(final T tenantId, final R rowKey, final Timestamper overrideTimestamper) throws Exception {

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            final long timestamp = (overrideTimestamper == null) ? timestamper.get() : overrideTimestamper.get();
            final byte[] rawRowKey = marshaller.toRowKeyBytes(tenantId, rowKey);

            PreparedStatement pstmt = con.prepareStatement(deleteRowSql);
            pstmt.setBytes(1, rawRowKey);
            pstmt.setLong(2, timestamp);
            pstmt.executeUpdate();
            pstmt.close();
            con.commit();
            counters.removed(1);
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Failed to remove. customer=" + tenantId + " key=" + rowKey, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    //TODO needs tests
    /**
     * Returns a List containing the values for a specified column for multiple rows. If a row did not have a value for that column (or if the row does not
     * exist), a null entry is inserted in the list instead.
     *
     * @param tenantId
     * @param rowKeys
     * @param columnKey
     * @param overrideNumberOfRetries
     * @param overrideConsistency
     * @return
     */
    @Override
    public List<V> multiRowGet(T tenantId, List<R> rowKeys, C columnKey, Integer overrideNumberOfRetries, Integer overrideConsistency) throws Exception {

        byte[][] rawRowKeyBytes = new byte[rowKeys.size()][];
        byte[] rawColumnKey;
        try {
            rawColumnKey = marshaller.toColumnKeyBytes(columnKey);

            int i = 0;
            for (R rowKey : rowKeys) {
                rawRowKeyBytes[i++] = marshaller.toRowKeyBytes(tenantId, rowKey);
            }
        } catch (Exception e) {
            throw e;
        }

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            PreparedStatement pstmt = con.prepareStatement(applyInBytes(selectRowValForRowInColSql, rawRowKeyBytes));
            pstmt.setBytes(1, rawColumnKey);

            ResultSet rs = pstmt.executeQuery();
            Map<R, V> resultMap = Maps.newHashMap();
            while (rs.next()) {
                TenantIdAndRow<T, R> tenantIdAndRow = marshaller.fromRowKeyBytes(rs.getBytes(1));
                V v = marshaller.fromValueBytes(rs.getBytes(2));
                resultMap.put(tenantIdAndRow.getRow(), v);
                counters.got(1);
            }

            List<V> values = Lists.newArrayListWithCapacity(rowKeys.size());
            for (R rowKey : rowKeys) {
                values.add(resultMap.get(rowKey));
            }
            return values;
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Failed to retrieve key. customer=" + tenantId + " keys=" + rowKeys + " columnName=" + columnKey, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    /**
     * Returns a List containing the values for specified columns for multiple rows (as a map from column to its value). If a row does not exist, a null entry
     * is inserted in the list instead. If there's no value for a specific column and a specific row, there would be no entry in the corresponding map.
     *
     * @param tenantId
     * @param rowKeys
     * @param columnKeys
     * @param overrideNumberOfRetries
     * @param overrideConsistency
     * @return
     */
    @Override
    public List<Map<C, V>> multiRowMultiGet(T tenantId, List<R> rowKeys, List<C> columnKeys, Integer overrideNumberOfRetries, Integer overrideConsistency)
        throws Exception {

        byte[][] rawRowKeyBytes = new byte[rowKeys.size()][];
        byte[][] rawColumnKeyBytes = new byte[columnKeys.size()][];
        try {
            int i = 0;
            for (R rowKey : rowKeys) {
                rawRowKeyBytes[i++] = marshaller.toRowKeyBytes(tenantId, rowKey);
            }

            i = 0;
            for (C columnKey : columnKeys) {
                rawColumnKeyBytes[i++] = marshaller.toColumnKeyBytes(columnKey);
            }
        } catch (Exception e) {
            throw e;
        }

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            PreparedStatement pstmt = con.prepareStatement(applyInBytes(selectRowColValForRowInColInSql, rawRowKeyBytes, rawColumnKeyBytes));

            ResultSet rs = pstmt.executeQuery();
            Table<R, C, V> resultTable = HashBasedTable.create();
            while (rs.next()) {
                TenantIdAndRow<T, R> tenantIdAndRow = marshaller.fromRowKeyBytes(rs.getBytes(1));
                C c = marshaller.fromColumnKeyBytes(rs.getBytes(2));
                V v = marshaller.fromValueBytes(rs.getBytes(3));
                resultTable.put(tenantIdAndRow.getRow(), c, v);
                counters.got(1);
            }
            rs.close();
            pstmt.close();

            List<Map<C, V>> values = Lists.newArrayListWithCapacity(rowKeys.size());
            for (R rowKey : rowKeys) {
                Map<C, V> rowMap = resultTable.row(rowKey);
                if (!rowMap.isEmpty()) {
                    values.add(rowMap);
                } else {
                    values.add(null);
                }
            }
            return values;
        } catch (RowColumnValueStoreMarshallerException ex) {
            LOG.error("Failed to retrieve key. customer=" + tenantId + " keys=" + rowKeys + " columnNames=" + columnKeys, ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    //TODO needs tests
    @Override
    public <TS> void multiRowGetAll(T tenantId, List<KeyedColumnValueCallbackStream<R, C, V, TS>> rowKeyCallbackStreamPairs) throws Exception {

        byte[][] rawRowKeyBytes = new byte[rowKeyCallbackStreamPairs.size()][];
        Map<R, CallbackStream<ColumnValueAndTimestamp<C, V, TS>>> callbacksMap = Maps.newHashMap();
        try {
            int i = 0;
            for (KeyedColumnValueCallbackStream<R, C, V, TS> pair : rowKeyCallbackStreamPairs) {
                rawRowKeyBytes[i++] = marshaller.toRowKeyBytes(tenantId, pair.getKey());
                callbacksMap.put(pair.getKey(), pair.getCallbackStream());
            }
        } catch (Exception e) {
            throw e;
        }

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            PreparedStatement pstmt = con.prepareStatement(applyInBytes(selectRowColValForRowInAscSql, rawRowKeyBytes));

            ResultSet rs = pstmt.executeQuery();
            R currentRow = null;
            CallbackStream<ColumnValueAndTimestamp<C, V, TS>> currentCallback = null;
            boolean skipRestOfRow = false;
            while (rs.next()) {
                TenantIdAndRow<T, R> tenantIdAndRow = marshaller.fromRowKeyBytes(rs.getBytes(1));
                R r = tenantIdAndRow.getRow();
                if (currentRow == null || !currentRow.equals(r)) {
                    if (currentCallback != null && !skipRestOfRow) {
                        try {
                            currentCallback.callback(null);
                        } catch (Exception ex) {
                            throw new CallbackStreamException(ex);
                        }
                    }

                    currentRow = r;
                    currentCallback = callbacksMap.get(r);
                    skipRestOfRow = false;
                } else if (skipRestOfRow) {
                    continue;
                }

                C c = marshaller.fromColumnKeyBytes(rs.getBytes(2));
                V v = marshaller.fromValueBytes(rs.getBytes(3));
                Long timestamp = rs.getLong(4);

                try {
                    ColumnValueAndTimestamp<C, V, TS> cvat = new ColumnValueAndTimestamp<>(c, v, (TS) timestamp);
                    if (currentCallback.callback(cvat) != cvat) {
                        skipRestOfRow = true;
                        continue;
                    }
                } catch (Exception ex) {
                    throw new CallbackStreamException(ex);
                }
                counters.got(1);
            }
            rs.close();
            pstmt.close();

            if (currentCallback != null && !skipRestOfRow) {
                try {
                    currentCallback.callback(null);
                } catch (Exception ex) {
                    throw new CallbackStreamException(ex);
                }
            }
        } catch (Exception ex) {
            LOG.error("Failed to retrieve key. customer=" + tenantId + " keys=" + rowKeyCallbackStreamPairs + " (all columns)", ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    //TODO needs tests
    @Override
    public <TS> void multiRowGetAll(List<TenantKeyedColumnValueCallbackStream<T, R, C, V, TS>> rowKeyCallbackStreamPairs) throws Exception {

        byte[][] rawRowKeyBytes = new byte[rowKeyCallbackStreamPairs.size()][];
        Map<R, CallbackStream<ColumnValueAndTimestamp<C, V, TS>>> callbacksMap = Maps.newHashMap();
        try {
            int i = 0;
            for (TenantKeyedColumnValueCallbackStream<T, R, C, V, TS> pair : rowKeyCallbackStreamPairs) {
                rawRowKeyBytes[i++] = marshaller.toRowKeyBytes(pair.getTenantId(), pair.getKey());
                callbacksMap.put(pair.getKey(), pair.getCallbackStream());
            }
        } catch (Exception e) {
            throw e;
        }

        Connection con = dataSource.getConnection();
        try {
            con.setAutoCommit(false);

            PreparedStatement pstmt = con.prepareStatement(applyInBytes(selectRowColValForRowInAscSql, rawRowKeyBytes));

            ResultSet rs = pstmt.executeQuery();
            R currentRow = null;
            CallbackStream<ColumnValueAndTimestamp<C, V, TS>> currentCallback = null;
            boolean skipRestOfRow = false;
            while (rs.next()) {
                TenantIdAndRow<T, R> tenantIdAndRow = marshaller.fromRowKeyBytes(rs.getBytes(1));
                R r = tenantIdAndRow.getRow();
                if (currentRow == null || !currentRow.equals(r)) {
                    if (currentCallback != null && !skipRestOfRow) {
                        try {
                            currentCallback.callback(null);
                        } catch (Exception ex) {
                            throw new CallbackStreamException(ex);
                        }
                    }

                    currentRow = r;
                    currentCallback = callbacksMap.get(r);
                    skipRestOfRow = false;
                } else if (skipRestOfRow) {
                    continue;
                }

                C c = marshaller.fromColumnKeyBytes(rs.getBytes(2));
                V v = marshaller.fromValueBytes(rs.getBytes(3));
                Long timestamp = rs.getLong(4);

                try {
                    ColumnValueAndTimestamp<C, V, TS> cvat = new ColumnValueAndTimestamp<>(c, v, (TS) timestamp);
                    if (currentCallback.callback(cvat) != cvat) {
                        skipRestOfRow = true;
                        continue;
                    }
                } catch (Exception ex) {
                    throw new CallbackStreamException(ex);
                }
                counters.got(1);
            }
            rs.close();
            pstmt.close();

            if (currentCallback != null && !skipRestOfRow) {
                try {
                    currentCallback.callback(null);
                } catch (Exception ex) {
                    throw new CallbackStreamException(ex);
                }
            }
        } catch (Exception ex) {
            LOG.error("Failed to retrieve key. keys=" + rowKeyCallbackStreamPairs + " (all columns)", ex);
            throw ex;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                LOG.error("Failed to close postgres connection!", e);
            }
        }
    }

    private String applyLimit(String sql, Long maxCount) {
        if (maxCount == null) {
            return sql;
        } else {
            return sql + " LIMIT " + maxCount;
        }
    }

    private String applyInBytes(String sql, byte[][]... bytesArrays) {
        for (byte[][] bytesArray : bytesArrays) {
            // once for size
            int size = 0;
            for (byte[] bytes : bytesArray) {
                size += (bytes.length * 2) + 5; // 2 hex per byte plus notation and comma, e.g. '\x00112233',
            }

            StringBuilder buf = new StringBuilder(size);
            boolean first = true;
            for (byte[] bytes : bytesArray) {
                if (first) {
                    first = false;
                } else {
                    buf.append(',');
                }
                buf.append("'\\\\x");
                buf.append(Hex.encodeHexString(bytes));
                buf.append('\'');
            }

            sql = sql.replaceFirst("%a", buf.toString());
        }
        return sql;
    }
}
