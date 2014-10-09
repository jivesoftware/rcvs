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
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import java.nio.ByteBuffer;

/**
 * Marshall Double to and from bytes
 */
public class DoubleTypeMarshaller implements TypeMarshaller<Double> {

    @Override
    public Double fromBytes(byte[] bytes) throws Exception {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    @Override
    public byte[] toBytes(Double t) throws Exception {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putDouble(t);
        return bb.array();
    }

    @Override
    public Double fromLexBytes(byte[] lexBytes) throws Exception {
        return UtilLexMarshaller.doubleFromLex(lexBytes);
    }

    @Override
    public byte[] toLexBytes(Double t) throws Exception {
        return UtilLexMarshaller.doubleToLex(t);
    }
}
