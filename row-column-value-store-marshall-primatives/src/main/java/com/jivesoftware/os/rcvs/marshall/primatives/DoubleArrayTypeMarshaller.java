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
package com.jivesoftware.os.rcvs.marshall.primatives;

import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 * Marshall Double to and from bytes
 */
public class DoubleArrayTypeMarshaller implements TypeMarshaller<double[]> {

    @Override
    public double[] fromBytes(byte[] bytes) throws Exception {

        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int length = bb.getInt();
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return new double[0];
        }
        double[] a = new double[length];
        for (int i = 0; i < length; i++) {
            a[i] = bb.getDouble();
        }
        return a;
    }

    @Override
    public byte[] toBytes(double[] t) throws Exception {
        if (t == null) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(-1);
            return bb.array();
        } else if (t.length == 0) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(0);
            return bb.array();
        } else {
            ByteBuffer bb = ByteBuffer.allocate(4 + (t.length * 8));
            bb.putInt(t.length);
            for (int i = 0; i < t.length; i++) {
                bb.putDouble(t[i]);
            }
            return bb.array();
        }
    }

    @Override
    public double[] fromLexBytes(byte[] lexBytes) throws Exception {
        return fromBytes(lexBytes);
    }

    @Override
    public byte[] toLexBytes(double[] t) throws Exception {
        return toBytes(t);
    }
}
