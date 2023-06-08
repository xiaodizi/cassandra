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

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

import io.github.xiaodizi.db.TypeSizes;
import io.github.xiaodizi.io.util.DataOutputPlus;
import io.github.xiaodizi.utils.obs.IBitSet;
import io.github.xiaodizi.utils.obs.OffHeapBitSet;

public final class BloomFilterSerializer
{
    private BloomFilterSerializer()
    {
    }

    public static void serialize(BloomFilter bf, DataOutputPlus out) throws IOException
    {
        out.writeInt(bf.hashCount);
        bf.bitset.serialize(out);
    }

    @SuppressWarnings("resource")
    public static <I extends InputStream & DataInput> BloomFilter deserialize(I in, boolean oldBfFormat) throws IOException
    {
        int hashes = in.readInt();
        IBitSet bs = OffHeapBitSet.deserialize(in, oldBfFormat);

        return new BloomFilter(hashes, bs);
    }

    /**
     * Calculates a serialized size of the given Bloom Filter
     * @param bf Bloom filter to calculate serialized size
     * @see io.github.xiaodizi.io.ISerializer#serialize(Object, io.github.xiaodizi.io.util.DataOutputPlus)
     *
     * @return serialized size of the given bloom filter
     */
    public static long serializedSize(BloomFilter bf)
    {
        int size = TypeSizes.sizeof(bf.hashCount); // hash count
        size += bf.bitset.serializedSize();
        return size;
    }
}
