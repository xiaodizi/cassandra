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

import java.util.*;
import java.util.function.LongPredicate;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.lifecycle.LifecycleTransaction;
import io.github.xiaodizi.db.SerializationHeader;
import io.github.xiaodizi.io.sstable.*;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.io.sstable.format.SSTableWriter;
import io.github.xiaodizi.io.sstable.metadata.MetadataCollector;
import io.github.xiaodizi.io.sstable.metadata.StatsMetadata;
import io.github.xiaodizi.io.util.File;
import io.github.xiaodizi.utils.FBUtilities;
import io.github.xiaodizi.utils.OutputHandler;

import static io.github.xiaodizi.utils.TimeUUID.Generator.nextTimeUUID;

public class Upgrader
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final File directory;

    private final CompactionController controller;
    private final CompactionStrategyManager strategyManager;
    private final long estimatedRows;

    private final OutputHandler outputHandler;

    public Upgrader(ColumnFamilyStore cfs, LifecycleTransaction txn, OutputHandler outputHandler)
    {
        this.cfs = cfs;
        this.transaction = txn;
        this.sstable = txn.onlyOne();
        this.outputHandler = outputHandler;

        this.directory = new File(sstable.getFilename()).parent();

        this.controller = new UpgradeController(cfs);

        this.strategyManager = cfs.getCompactionStrategyManager();
        long estimatedTotalKeys = Math.max(cfs.metadata().params.minIndexInterval, SSTableReader.getApproximateKeyCount(Arrays.asList(this.sstable)));
        long estimatedSSTables = Math.max(1, SSTableReader.getTotalBytes(Arrays.asList(this.sstable)) / strategyManager.getMaxSSTableBytes());
        this.estimatedRows = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
    }

    private SSTableWriter createCompactionWriter(StatsMetadata metadata)
    {
        MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.getComparator());
        sstableMetadataCollector.sstableLevel(sstable.getSSTableLevel());
        return SSTableWriter.create(cfs.newSSTableDescriptor(directory),
                                    estimatedRows,
                                    metadata.repairedAt,
                                    metadata.pendingRepair,
                                    metadata.isTransient,
                                    cfs.metadata,
                                    sstableMetadataCollector,
                                    SerializationHeader.make(cfs.metadata(), Sets.newHashSet(sstable)),
                                    cfs.indexManager.listIndexes(),
                                    transaction);
    }

    public void upgrade(boolean keepOriginals)
    {
        outputHandler.output("Upgrading " + sstable);
        int nowInSec = FBUtilities.nowInSeconds();
        try (SSTableRewriter writer = SSTableRewriter.construct(cfs, transaction, keepOriginals, CompactionTask.getMaxDataAge(transaction.originals()));
             AbstractCompactionStrategy.ScannerList scanners = strategyManager.getScanners(transaction.originals());
             CompactionIterator iter = new CompactionIterator(transaction.opType(), scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(createCompactionWriter(sstable.getSSTableMetadata()));
            while (iter.hasNext())
                writer.append(iter.next());

            writer.finish();
            outputHandler.output("Upgrade of " + sstable + " complete.");
        }
        catch (Exception e)
        {
            Throwables.propagate(e);
        }
        finally
        {
            controller.close();
        }
    }

    private static class UpgradeController extends CompactionController
    {
        public UpgradeController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }
}

