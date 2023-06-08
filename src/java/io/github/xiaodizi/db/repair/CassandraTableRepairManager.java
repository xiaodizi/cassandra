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

package io.github.xiaodizi.db.repair;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.google.common.base.Predicate;

import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.compaction.CompactionManager;
import io.github.xiaodizi.dht.Bounds;
import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.metrics.TopPartitionTracker;
import io.github.xiaodizi.repair.TableRepairManager;
import io.github.xiaodizi.repair.ValidationPartitionIterator;
import io.github.xiaodizi.repair.NoSuchRepairSessionException;
import io.github.xiaodizi.utils.TimeUUID;
import io.github.xiaodizi.service.ActiveRepairService;

public class CassandraTableRepairManager implements TableRepairManager
{
    private final ColumnFamilyStore cfs;

    public CassandraTableRepairManager(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    @Override
    public ValidationPartitionIterator getValidationIterator(Collection<Range<Token>> ranges, TimeUUID parentId, TimeUUID sessionID, boolean isIncremental, int nowInSec, TopPartitionTracker.Collector topPartitionCollector) throws IOException, NoSuchRepairSessionException
    {
        return new CassandraValidationIterator(cfs, ranges, parentId, sessionID, isIncremental, nowInSec, topPartitionCollector);
    }

    @Override
    public Future<?> submitValidation(Callable<Object> validation)
    {
        return CompactionManager.instance.submitValidation(validation);
    }

    @Override
    public void incrementalSessionCompleted(TimeUUID sessionID)
    {
        CompactionManager.instance.submitBackground(cfs);
    }

    @Override
    public synchronized void snapshot(String name, Collection<Range<Token>> ranges, boolean force)
    {
        try
        {
            ActiveRepairService.instance.snapshotExecutor.submit(() -> {
                if (force || !cfs.snapshotExists(name))
                {
                    cfs.snapshot(name, new Predicate<SSTableReader>()
                    {
                        public boolean apply(SSTableReader sstable)
                        {
                            return sstable != null &&
                                   !sstable.metadata().isIndex() && // exclude SSTables from 2i
                                   new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges);
                        }
                    }, true, false); //ephemeral snapshot, if repair fails, it will be cleaned next startup
                }
            }).get();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Unable to take a snapshot %s on %s.%s", name, cfs.metadata.keyspace, cfs.metadata.name), ex);
        }

    }
}
