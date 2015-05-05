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
package com.jivesoftware.os.rcvs.api;

/**
 *
 * @author jonathan
 */
public class TenantIdAndRow<T, R> {

    private final T tenantId;
    private final R row;

    public TenantIdAndRow(T tenantId, R row) {
        this.tenantId = tenantId;
        this.row = row;
    }

    public R getRow() {
        return row;
    }

    public T getTenantId() {
        return tenantId;
    }

    @Override
    public String toString() {
        return "TenantIdAndRow{"
            + "tenantId=" + tenantId
            + ", row=" + KeyToStringUtils.keyToString(row) + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TenantIdAndRow<?, ?> other = (TenantIdAndRow<?, ?>) obj;
        if (this.tenantId != other.tenantId && (this.tenantId == null || !this.tenantId.equals(other.tenantId))) {
            return false;
        }
        if (this.row != other.row && (this.row == null || !this.row.equals(other.row))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + (this.tenantId != null ? this.tenantId.hashCode() : 0);
        hash = 41 * hash + (this.row != null ? this.row.hashCode() : 0);
        return hash;
    }
}
