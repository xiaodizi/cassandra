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
package io.github.xiaodizi.db.memtable;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xiaodizi.db.BufferDecoratedKey;
import io.github.xiaodizi.db.DataRange;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.PartitionPosition;
import io.github.xiaodizi.db.Slices;
import io.github.xiaodizi.db.commitlog.CommitLogPosition;
import io.github.xiaodizi.db.filter.ClusteringIndexFilter;
import io.github.xiaodizi.db.filter.ColumnFilter;
import io.github.xiaodizi.db.partitions.AbstractBTreePartition;
import io.github.xiaodizi.db.partitions.AbstractUnfilteredPartitionIterator;
import io.github.xiaodizi.db.partitions.AtomicBTreePartition;
import io.github.xiaodizi.db.partitions.Partition;
import io.github.xiaodizi.db.partitions.PartitionUpdate;
import io.github.xiaodizi.db.partitions.UnfilteredPartitionIterator;
import io.github.xiaodizi.db.rows.UnfilteredRowIterator;
import io.github.xiaodizi.dht.AbstractBounds;
import io.github.xiaodizi.dht.Bounds;
import io.github.xiaodizi.dht.IncludingExcludingBounds;
import io.github.xiaodizi.dht.Murmur3Partitioner.LongToken;
import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.index.transactions.UpdateTransaction;
import io.github.xiaodizi.io.sstable.format.SSTableReadsListener;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.schema.TableMetadataRef;
import io.github.xiaodizi.utils.ByteBufferUtil;
import io.github.xiaodizi.utils.ObjectSizes;
import io.github.xiaodizi.utils.concurrent.OpOrder;
import io.github.xiaodizi.utils.memory.Cloner;
import io.github.xiaodizi.utils.memory.MemtableAllocator;

import static io.github.xiaodizi.config.CassandraRelevantProperties.MEMTABLE_OVERHEAD_COMPUTE_STEPS;
import static io.github.xiaodizi.config.CassandraRelevantProperties.MEMTABLE_OVERHEAD_SIZE;

public class SkipListMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(SkipListMemtable.class);

    public static final Factory FACTORY = SkipListMemtableFactory.INSTANCE;

    protected static final int ROW_OVERHEAD_HEAP_SIZE;
    static
    {
        int userDefinedOverhead = MEMTABLE_OVERHEAD_SIZE.getInt(-1);
        if (userDefinedOverhead > 0)
            ROW_OVERHEAD_HEAP_SIZE = userDefinedOverhead;
        else
            ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(MEMTABLE_OVERHEAD_COMPUTE_STEPS.getInt());
    }

    // We index the memtable by PartitionPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();

    private final AtomicLong liveDataSize = new AtomicLong(0);

    protected SkipListMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
    }

    @Override
    protected Factory factory()
    {
        return FACTORY;
    }

    @Override
    public boolean isClean()
    {
        return partitions.isEmpty();
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        Cloner cloner = allocator.cloner(opGroup);
        AtomicBTreePartition previous = partitions.get(update.partitionKey());

        long initialSize = 0;
        if (previous == null)
        {
            final DecoratedKey cloneKey = cloner.clone(update.partitionKey());
            AtomicBTreePartition empty = new AtomicBTreePartition(metadata, cloneKey, allocator);
            // We'll add the columns later. This avoids wasting works if we get beaten in the putIfAbsent
            previous = partitions.putIfAbsent(cloneKey, empty);
            if (previous == null)
            {
                previous = empty;
                // allocate the row overhead after the fact; this saves over allocating and having to free after, but
                // means we can overshoot our declared limit.
                int overhead = (int) (cloneKey.getToken().getHeapSize() + ROW_OVERHEAD_HEAP_SIZE);
                allocator.onHeap().allocate(overhead, opGroup);
                initialSize = 8;
            }
        }

        long[] pair = previous.addAllWithSizeDelta(update, cloner, opGroup, indexer);
        updateMin(minTimestamp, update.stats().minTimestamp);
        updateMin(minLocalDeletionTime, update.stats().minLocalDeletionTime);
        liveDataSize.addAndGet(initialSize + pair[0]);
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        return pair[1];
    }

    @Override
    public long partitionCount()
    {
        return partitions.size();
    }

    @Override
    public MemtableUnfilteredPartitionIterator partitionIterator(final ColumnFilter columnFilter,
                                                                 final DataRange dataRange,
                                                                 SSTableReadsListener readsListener)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;

        boolean isBound = keyRange instanceof Bounds;
        boolean includeLeft = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeRight = isBound || keyRange instanceof Range;
        Map<PartitionPosition, AtomicBTreePartition> subMap = getPartitionsSubMap(left,
                                                                                  includeLeft,
                                                                                  right,
                                                                                  includeRight);

        return new MemtableUnfilteredPartitionIterator(metadata.get(), subMap, columnFilter, dataRange);
        // readsListener is ignored as it only accepts sstable signals
    }

    private Map<PartitionPosition, AtomicBTreePartition> getPartitionsSubMap(PartitionPosition left,
                                                                             boolean includeLeft,
                                                                             PartitionPosition right,
                                                                             boolean includeRight)
    {
        if (left != null && left.isMinimum())
            left = null;
        if (right != null && right.isMinimum())
            right = null;

        try
        {
            if (left == null)
                return right == null ? partitions : partitions.headMap(right, includeRight);
            else
                return right == null
                       ? partitions.tailMap(left, includeLeft)
                       : partitions.subMap(left, includeLeft, right, includeRight);
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Invalid range requested {} - {}", left, right);
            throw e;
        }
    }

    Partition getPartition(DecoratedKey key)
    {
        return partitions.get(key);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        else
            return p.unfilteredIterator(selectedColumns, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        Partition p = getPartition(key);
        return p != null ? p.unfilteredIterator() : null;
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        try (final OpOrder.Group group = new OpOrder().start())
        {
            int rowOverhead;
            MemtableAllocator allocator = MEMORY_POOL.newAllocator("");
            Cloner cloner = allocator.cloner(group);
            ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap<>();
            final Object val = new Object();
            for (int i = 0 ; i < count ; i++)
                partitions.put(cloner.clone(new BufferDecoratedKey(new LongToken(i), ByteBufferUtil.EMPTY_BYTE_BUFFER)), val);
            double avgSize = ObjectSizes.measureDeep(partitions) / (double) count;
            rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
            rowOverhead -= ObjectSizes.measureDeep(new LongToken(0));
            rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
            rowOverhead += AbstractBTreePartition.HOLDER_UNSHARED_HEAP_SIZE;
            allocator.setDiscarding();
            allocator.setDiscarded();
            return rowOverhead;
        }
    }

    @Override
    public FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Map<PartitionPosition, AtomicBTreePartition> toFlush = getPartitionsSubMap(from, true, to, false);
        long keysSize = 0;
        long keyCount = 0;

        boolean trackContention = logger.isTraceEnabled();
        if (trackContention)
        {
            int heavilyContendedRowCount = 0;

            for (AtomicBTreePartition partition : toFlush.values())
            {
                keysSize += partition.partitionKey().getKey().remaining();
                ++keyCount;
                if (partition.useLock())
                    heavilyContendedRowCount++;
            }

            if (heavilyContendedRowCount > 0)
                logger.trace("High update contention in {}/{} partitions of {} ", heavilyContendedRowCount, toFlush.size(), SkipListMemtable.this);
        }
        else
        {
            for (PartitionPosition key : toFlush.keySet())
            {
                //  make sure we don't write non-sensical keys
                assert key instanceof DecoratedKey;
                keysSize += ((DecoratedKey) key).getKey().remaining();
                ++keyCount;
            }
        }
        final long partitionKeysSize = keysSize;
        final long partitionCount = keyCount;

        return new AbstractFlushablePartitionSet<AtomicBTreePartition>()
        {
            @Override
            public Memtable memtable()
            {
                return SkipListMemtable.this;
            }

            @Override
            public PartitionPosition from()
            {
                return from;
            }

            @Override
            public PartitionPosition to()
            {
                return to;
            }

            @Override
            public long partitionCount()
            {
                return partitionCount;
            }

            @Override
            public Iterator<AtomicBTreePartition> iterator()
            {
                return toFlush.values().iterator();
            }

            @Override
            public long partitionKeysSize()
            {
                return partitionKeysSize;
            }
        };
    }


    private static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements UnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        MemtableUnfilteredPartitionIterator(TableMetadata metadata, Map<PartitionPosition, AtomicBTreePartition> map, ColumnFilter columnFilter, DataRange dataRange)
        {
            this.metadata = metadata;
            this.iter = map.entrySet().iterator();
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            Map.Entry<PartitionPosition, AtomicBTreePartition> entry = iter.next();
            // Actual stored key should be true DecoratedKey
            assert entry.getKey() instanceof DecoratedKey;
            DecoratedKey key = (DecoratedKey)entry.getKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, entry.getValue());
        }
    }

    @Override
    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1024L * 1024 * 1024 * 1024 * 1024);
    }
}
