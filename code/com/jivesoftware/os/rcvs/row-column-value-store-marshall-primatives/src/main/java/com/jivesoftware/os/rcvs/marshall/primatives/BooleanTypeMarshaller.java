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
 * Marshall Boolean to and from bytes
 */
public class BooleanTypeMarshaller implements TypeMarshaller<Boolean> {

    @Override
    public Boolean fromBytes(byte[] bytes) throws Exception {
        int i = ByteBuffer.wrap(bytes).getInt();

        return i == 1 ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override
    public byte[] toBytes(Boolean t) throws Exception {
        int i = t.equals(Boolean.TRUE) ? 1 : 0;
        ByteBuffer bb = ByteBuffer.allocate(8);

        bb.putInt(i);

        return bb.array();
    }

    @Override
    public Boolean fromLexBytes(byte[] lexBytes) throws Exception {
        return fromBytes(lexBytes);
    }

    @Override
    public byte[] toLexBytes(Boolean t) throws Exception {
        return toBytes(t);
    }
}
