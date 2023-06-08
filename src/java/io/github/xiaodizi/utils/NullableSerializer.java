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

package io.github.xiaodizi.utils;

import java.io.IOException;

import io.github.xiaodizi.db.TypeSizes;
import io.github.xiaodizi.io.IVersionedSerializer;
import io.github.xiaodizi.io.util.DataInputPlus;
import io.github.xiaodizi.io.util.DataOutputPlus;

public class NullableSerializer
{

    public static <T> void serializeNullable(IVersionedSerializer<T> serializer, T value, DataOutputPlus out, int version) throws IOException
    {
        out.writeBoolean(value != null);
        if (value != null)
            serializer.serialize(value, out, version);
    }

    public static <T> T deserializeNullable(IVersionedSerializer<T> serializer, DataInputPlus in, int version) throws IOException
    {
        return in.readBoolean() ? serializer.deserialize(in, version) : null;
    }

    public static <T> long serializedSizeNullable(IVersionedSerializer<T> serializer, T value, int version)
    {
        return value != null
                ? TypeSizes.sizeof(true) + serializer.serializedSize(value, version)
                : TypeSizes.sizeof(false);
    }

    public static <T> IVersionedSerializer<T> wrap(IVersionedSerializer<T> wrap)
    {
        return new IVersionedSerializer<T>() {
            public void serialize(T t, DataOutputPlus out, int version) throws IOException
            {
                serializeNullable(wrap, t, out, version);
            }

            public T deserialize(DataInputPlus in, int version) throws IOException
            {
                return deserializeNullable(wrap, in, version);
            }

            public long serializedSize(T t, int version)
            {
                return serializedSizeNullable(wrap, t, version);
            }
        };
    }

}
