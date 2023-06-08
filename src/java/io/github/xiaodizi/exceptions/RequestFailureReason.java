/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.xiaodizi.exceptions;

import java.io.IOException;

import com.google.common.primitives.Ints;

import io.github.xiaodizi.db.filter.TombstoneOverwhelmingException;
import io.github.xiaodizi.io.IVersionedSerializer;
import io.github.xiaodizi.io.util.DataInputPlus;
import io.github.xiaodizi.io.util.DataOutputPlus;
import io.github.xiaodizi.utils.vint.VIntCoding;

import static java.lang.Math.max;
import static io.github.xiaodizi.net.MessagingService.VERSION_40;

public enum RequestFailureReason
{
    UNKNOWN                  (0),
    READ_TOO_MANY_TOMBSTONES (1),
    TIMEOUT                  (2),
    INCOMPATIBLE_SCHEMA      (3),
    READ_SIZE                (4),
    NODE_DOWN                (5);

    public static final Serializer serializer = new Serializer();

    public final int code;

    RequestFailureReason(int code)
    {
        this.code = code;
    }

    private static final RequestFailureReason[] codeToReasonMap;

    static
    {
        RequestFailureReason[] reasons = values();

        int max = -1;
        for (RequestFailureReason r : reasons)
            max = max(r.code, max);

        RequestFailureReason[] codeMap = new RequestFailureReason[max + 1];

        for (RequestFailureReason reason : reasons)
        {
            if (codeMap[reason.code] != null)
                throw new RuntimeException("Two RequestFailureReason-s that map to the same code: " + reason.code);
            codeMap[reason.code] = reason;
        }

        codeToReasonMap = codeMap;
    }

    public static RequestFailureReason fromCode(int code)
    {
        if (code < 0)
            throw new IllegalArgumentException("RequestFailureReason code must be non-negative (got " + code + ')');

        // be forgiving and return UNKNOWN if we aren't aware of the code - for forward compatibility
        return code < codeToReasonMap.length ? codeToReasonMap[code] : UNKNOWN;
    }

    public static RequestFailureReason forException(Throwable t)
    {
        if (t instanceof TombstoneOverwhelmingException)
            return READ_TOO_MANY_TOMBSTONES;

        if (t instanceof IncompatibleSchemaException)
            return INCOMPATIBLE_SCHEMA;

        return UNKNOWN;
    }

    public static final class Serializer implements IVersionedSerializer<RequestFailureReason>
    {
        private Serializer()
        {
        }

        public void serialize(RequestFailureReason reason, DataOutputPlus out, int version) throws IOException
        {
            if (version < VERSION_40)
                out.writeShort(reason.code);
            else
                out.writeUnsignedVInt(reason.code);
        }

        public RequestFailureReason deserialize(DataInputPlus in, int version) throws IOException
        {
            return fromCode(version < VERSION_40 ? in.readUnsignedShort() : Ints.checkedCast(in.readUnsignedVInt()));
        }

        public long serializedSize(RequestFailureReason reason, int version)
        {
            return version < VERSION_40 ? 2 : VIntCoding.computeVIntSize(reason.code);
        }
    }
}
