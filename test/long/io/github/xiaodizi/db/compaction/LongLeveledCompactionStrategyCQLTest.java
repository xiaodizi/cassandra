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

package io.github.xiaodizi.db.compaction;

import com.google.common.util.concurrent.Uninterruptibles;
import io.github.xiaodizi.config.Config;
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.cql3.CQLTester;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.service.StorageService;
import io.github.xiaodizi.utils.Hex;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.xiaodizi.utils.Clock.Global.currentTimeMillis;

public class LongLeveledCompactionStrategyCQLTest extends CQLTester
{

    @Test
    public void stressTestCompactionStrategyManager() throws ExecutionException, InterruptedException
    {
        System.setProperty(Config.PROPERTY_PREFIX + "test.strict_lcs_checks", "true");
        // flush/compact tons of sstables, invalidate token metadata in a loop to make CSM reload the strategies
        createTable("create table %s (id int primary key, i text) with compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':1}");
        ExecutorService es = Executors.newSingleThreadExecutor();
        DatabaseDescriptor.setConcurrentCompactors(8);
        AtomicBoolean stop = new AtomicBoolean(false);
        long start = currentTimeMillis();
        try
        {
            Random r = new Random();
            Future<?> writes = es.submit(() -> {

                byte[] b = new byte[1024];
                while (!stop.get())
                {

                    for (int i = 0 ; i < 100; i++)
                    {
                        try
                        {
                            r.nextBytes(b);
                            String s = Hex.bytesToHex(b);
                            execute("insert into %s (id, i) values (?,?)", r.nextInt(), s);
                        }
                        catch (Throwable throwable)
                        {
                            throw new RuntimeException(throwable);
                        }
                    }
                    getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                    Uninterruptibles.sleepUninterruptibly(r.nextInt(200), TimeUnit.MILLISECONDS);
                }
            });

            while(currentTimeMillis() - start < TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES))
            {
                StorageService.instance.getTokenMetadata().invalidateCachedRings();
                Uninterruptibles.sleepUninterruptibly(r.nextInt(1000), TimeUnit.MILLISECONDS);
            }

            stop.set(true);
            writes.get();
        }
        finally
        {
            es.shutdown();
        }
    }
}
