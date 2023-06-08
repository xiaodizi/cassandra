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

import io.github.xiaodizi.Util;
import io.github.xiaodizi.cache.AutoSavingCache;
import io.github.xiaodizi.cache.KeyCacheKey;
import io.github.xiaodizi.cql3.CQLTester;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.RowIndexEntry;
import io.github.xiaodizi.db.RowUpdateBuilder;
import io.github.xiaodizi.db.marshal.AsciiType;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.schema.ColumnMetadata;
import io.github.xiaodizi.service.CacheService;
import io.github.xiaodizi.utils.ByteBufferUtil;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CacheLoaderBench extends CQLTester
{
    private static final int numSSTables = 1000;
    private final Random random = new Random();

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.prepareServer();
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table1 = createTable(keyspace, "CREATE TABLE %s (key text PRIMARY KEY, val text)");
        String table2 = createTable(keyspace, "CREATE TABLE %s (key text PRIMARY KEY, val text)");


        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs1 = Keyspace.open(keyspace).getColumnFamilyStore(table1);
        ColumnFamilyStore cfs2 = Keyspace.open(keyspace).getColumnFamilyStore(table2);
        cfs1.disableAutoCompaction();
        cfs2.disableAutoCompaction();

        // Write a bunch of sstables to both cfs1 and cfs2

        List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>(2);
        columnFamilyStores.add(cfs1);
        columnFamilyStores.add(cfs2);

        logger.info("Creating {} sstables", numSSTables);
        for (ColumnFamilyStore cfs: columnFamilyStores)
        {
            cfs.truncateBlocking();
            for (int i = 0; i < numSSTables ; i++)
            {
                ColumnMetadata colDef = ColumnMetadata.regularColumn(cfs.metadata(), ByteBufferUtil.bytes("val"), AsciiType.instance);
                RowUpdateBuilder rowBuilder = new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis() + random.nextInt(), "key");
                rowBuilder.add(colDef, "val1");
                rowBuilder.build().apply();
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            }

            Assert.assertEquals(numSSTables, cfs.getLiveSSTables().size());

            // preheat key cache
            for (SSTableReader sstable : cfs.getLiveSSTables())
                sstable.getPosition(Util.dk("key"), SSTableReader.Operator.EQ);
        }

        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = CacheService.instance.keyCache;

        // serialize to file
        keyCache.submitWrite(keyCache.size()).get();
    }

    @Setup(Level.Invocation)
    public void setupKeyCache()
    {
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = CacheService.instance.keyCache;
        keyCache.clear();
    }

    @TearDown(Level.Trial)
    public void teardown()
    {
        CQLTester.cleanup();
        CQLTester.tearDownClass();
    }

    @Benchmark
    public void keyCacheLoadTest() throws Throwable
    {
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = CacheService.instance.keyCache;

        keyCache.loadSavedAsync().get();
    }
}
