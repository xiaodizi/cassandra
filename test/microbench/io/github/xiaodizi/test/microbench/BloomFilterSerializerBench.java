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

package io.github.xiaodizi.test.microbench;

import io.github.xiaodizi.db.BufferDecoratedKey;
import io.github.xiaodizi.dht.Murmur3Partitioner;
import io.github.xiaodizi.io.util.*;
import io.github.xiaodizi.utils.*;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 4, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@State(Scope.Benchmark)
public class BloomFilterSerializerBench
{

    @Param({"1", "10", "100", "1024"})
    private long numElemsInK;

    @Param({"true", "false"})
    public boolean oldBfFormat;

    static final IFilter.FilterKey wrap(ByteBuffer buf)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(0L), buf);
    }

    private ByteBuffer testVal = ByteBuffer.wrap(new byte[] { 0, 1});

    @Benchmark
    public void serializationTest() throws IOException
    {
        File file = FileUtils.createTempFile("bloomFilterTest-", ".dat");
        try
        {
            BloomFilter filter = (BloomFilter) FilterFactory.getFilter(numElemsInK * 1024, 0.01d);
            filter.add(wrap(testVal));
            DataOutputStreamPlus out = new FileOutputStreamPlus(file);
            if (oldBfFormat)
                SerializationsTest.serializeOldBfFormat(filter, out);
            else
                BloomFilterSerializer.serialize(filter, out);
            out.close();
            filter.close();

            FileInputStreamPlus in = new FileInputStreamPlus(file);
            BloomFilter filter2 = BloomFilterSerializer.deserialize(in, oldBfFormat);
            FileUtils.closeQuietly(in);
            filter2.close();
        }
        finally
        {
            file.tryDelete();
        }
    }

}
