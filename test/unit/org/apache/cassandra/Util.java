package org.apache.cassandra;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOError;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.cql3.ColumnIdentifier;
import io.github.xiaodizi.db.AbstractReadCommandBuilder;
import io.github.xiaodizi.db.Clustering;
import io.github.xiaodizi.db.ClusteringComparator;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.DeletionTime;
import io.github.xiaodizi.db.Directories;
import io.github.xiaodizi.db.Directories.DataDirectory;
import io.github.xiaodizi.db.DisallowedDirectories;
import io.github.xiaodizi.db.IMutation;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.Mutation;
import io.github.xiaodizi.db.PartitionPosition;
import io.github.xiaodizi.db.PartitionRangeReadCommand;
import io.github.xiaodizi.db.ReadCommand;
import io.github.xiaodizi.db.ReadExecutionController;
import io.github.xiaodizi.db.compaction.AbstractCompactionTask;
import io.github.xiaodizi.db.compaction.ActiveCompactionsTracker;
import io.github.xiaodizi.db.compaction.CompactionManager;
import io.github.xiaodizi.db.compaction.CompactionTasks;
import io.github.xiaodizi.db.compaction.OperationType;
import io.github.xiaodizi.db.lifecycle.LifecycleTransaction;
import io.github.xiaodizi.db.marshal.AbstractType;
import io.github.xiaodizi.db.marshal.AsciiType;
import io.github.xiaodizi.db.marshal.Int32Type;
import io.github.xiaodizi.db.partitions.FilteredPartition;
import io.github.xiaodizi.db.partitions.ImmutableBTreePartition;
import io.github.xiaodizi.db.partitions.Partition;
import io.github.xiaodizi.db.partitions.PartitionIterator;
import io.github.xiaodizi.db.partitions.PartitionUpdate;
import io.github.xiaodizi.db.partitions.UnfilteredPartitionIterator;
import io.github.xiaodizi.db.rows.AbstractUnfilteredRowIterator;
import io.github.xiaodizi.db.rows.BTreeRow;
import io.github.xiaodizi.db.rows.BufferCell;
import io.github.xiaodizi.db.rows.Cell;
import io.github.xiaodizi.db.rows.Cells;
import io.github.xiaodizi.db.rows.EncodingStats;
import io.github.xiaodizi.db.rows.Row;
import io.github.xiaodizi.db.rows.RowIterator;
import io.github.xiaodizi.db.rows.Rows;
import io.github.xiaodizi.db.rows.Unfiltered;
import io.github.xiaodizi.db.rows.UnfilteredRowIterator;
import io.github.xiaodizi.db.view.TableViews;
import io.github.xiaodizi.dht.IPartitioner;
import io.github.xiaodizi.dht.RandomPartitioner.BigIntegerToken;
import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.gms.ApplicationState;
import io.github.xiaodizi.gms.Gossiper;
import io.github.xiaodizi.gms.VersionedValue;
import io.github.xiaodizi.io.sstable.Descriptor;
import io.github.xiaodizi.io.sstable.SSTableId;
import io.github.xiaodizi.io.sstable.SSTableLoader;
import io.github.xiaodizi.io.sstable.SequenceBasedSSTableId;
import io.github.xiaodizi.io.sstable.UUIDBasedSSTableId;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.io.util.File;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.locator.Replica;
import io.github.xiaodizi.locator.ReplicaCollection;
import io.github.xiaodizi.net.MessagingService;
import io.github.xiaodizi.schema.ColumnMetadata;
import io.github.xiaodizi.schema.Schema;
import io.github.xiaodizi.schema.TableId;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.schema.TableMetadataRef;
import io.github.xiaodizi.service.StorageService;
import io.github.xiaodizi.service.pager.PagingState;
import io.github.xiaodizi.streaming.StreamResultFuture;
import io.github.xiaodizi.streaming.StreamState;
import io.github.xiaodizi.transport.ProtocolVersion;
import io.github.xiaodizi.utils.ByteBufferUtil;
import io.github.xiaodizi.utils.CassandraVersion;
import io.github.xiaodizi.utils.CounterId;
import io.github.xiaodizi.utils.FBUtilities;
import io.github.xiaodizi.utils.FilterFactory;
import io.github.xiaodizi.utils.OutputHandler;
import io.github.xiaodizi.utils.Throwables;
import org.awaitility.Awaitility;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Util
{
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    private static List<UUID> hostIdPool = new ArrayList<>();

    public static IPartitioner testPartitioner()
    {
        return DatabaseDescriptor.getPartitioner();
    }

    public static DecoratedKey dk(String key)
    {
        return testPartitioner().decorateKey(ByteBufferUtil.bytes(key));
    }

    public static DecoratedKey dk(String key, AbstractType<?> type)
    {
        return testPartitioner().decorateKey(type.fromString(key));
    }

    public static DecoratedKey dk(ByteBuffer key)
    {
        return testPartitioner().decorateKey(key);
    }

    public static PartitionPosition rp(String key)
    {
        return rp(key, testPartitioner());
    }

    public static PartitionPosition rp(String key, IPartitioner partitioner)
    {
        return PartitionPosition.ForKey.get(ByteBufferUtil.bytes(key), partitioner);
    }

    public static Clustering<?> clustering(ClusteringComparator comparator, Object... o)
    {
        return comparator.make(o);
    }

    public static Token token(int key)
    {
        return testPartitioner().getToken(ByteBufferUtil.bytes(key));
    }

    public static Token token(String key)
    {
        return testPartitioner().getToken(ByteBufferUtil.bytes(key));
    }

    public static Range<PartitionPosition> range(String left, String right)
    {
        return new Range<>(rp(left), rp(right));
    }

    public static Range<PartitionPosition> range(IPartitioner p, String left, String right)
    {
        return new Range<>(rp(left, p), rp(right, p));
    }

    //Test helper to make an iterator iterable once
    public static <T> Iterable<T> once(final Iterator<T> source)
    {
        return new Iterable<T>()
        {
            private AtomicBoolean exhausted = new AtomicBoolean();
            public Iterator<T> iterator()
            {
                Preconditions.checkState(!exhausted.getAndSet(true));
                return source;
            }
        };
    }

    public static ByteBuffer getBytes(long v)
    {
        byte[] bytes = new byte[8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(v);
        bb.rewind();
        return bb;
    }

    public static ByteBuffer getBytes(int v)
    {
        byte[] bytes = new byte[4];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putInt(v);
        bb.rewind();
        return bb;
    }

    /**
     * Writes out a bunch of mutations for a single column family.
     *
     * @param mutations A group of Mutations for the same keyspace and column family.
     * @return The ColumnFamilyStore that was used.
     */
    public static ColumnFamilyStore writeColumnFamily(List<Mutation> mutations)
    {
        IMutation first = mutations.get(0);
        String keyspaceName = first.getKeyspaceName();
        TableId tableId = first.getTableIds().iterator().next();

        for (Mutation rm : mutations)
            rm.applyUnsafe();

        ColumnFamilyStore store = Keyspace.open(keyspaceName).getColumnFamilyStore(tableId);
        Util.flush(store);
        return store;
    }

    public static boolean equalsCounterId(CounterId n, ByteBuffer context, int offset)
    {
        return CounterId.wrap(context, context.position() + offset).equals(n);
    }

    /**
     * Creates initial set of nodes and tokens. Nodes are added to StorageService as 'normal'
     */
    public static void createInitialRing(StorageService ss, IPartitioner partitioner, List<Token> endpointTokens,
                                         List<Token> keyTokens, List<InetAddressAndPort> hosts, List<UUID> hostIds, int howMany)
        throws UnknownHostException
    {
        // Expand pool of host IDs as necessary
        for (int i = hostIdPool.size(); i < howMany; i++)
            hostIdPool.add(UUID.randomUUID());

        boolean endpointTokenPrefilled = endpointTokens != null && !endpointTokens.isEmpty();
        for (int i=0; i<howMany; i++)
        {
            if(!endpointTokenPrefilled)
                endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            keyTokens.add(new BigIntegerToken(String.valueOf(10 * i + 5)));
            hostIds.add(hostIdPool.get(i));
        }

        for (int i=0; i<endpointTokens.size(); i++)
        {
            InetAddressAndPort ep = InetAddressAndPort.getByName("127.0.0." + String.valueOf(i + 1));
            Gossiper.instance.initializeNodeUnsafe(ep, hostIds.get(i), MessagingService.current_version, 1);
            Gossiper.instance.injectApplicationState(ep, ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(endpointTokens.get(i))));
            ss.onChange(ep,
                        ApplicationState.STATUS_WITH_PORT,
                        new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(endpointTokens.get(i))));
            ss.onChange(ep,
                        ApplicationState.STATUS,
                        new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(endpointTokens.get(i))));
            hosts.add(ep);
        }

        // check that all nodes are in token metadata
        for (int i=0; i<endpointTokens.size(); ++i)
            assertTrue(ss.getTokenMetadata().isMember(hosts.get(i)));
    }

    public static Future<?> compactAll(ColumnFamilyStore cfs, int gcBefore)
    {
        List<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            descriptors.add(sstable.descriptor);
        return CompactionManager.instance.submitUserDefined(cfs, descriptors, gcBefore);
    }

    public static void compact(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        int gcBefore = cfs.gcBefore(FBUtilities.nowInSeconds());
        try (CompactionTasks tasks = cfs.getCompactionStrategyManager().getUserDefinedTasks(sstables, gcBefore))
        {
            for (AbstractCompactionTask task : tasks)
                task.execute(ActiveCompactionsTracker.NOOP);
        }
    }

    public static void expectEOF(Callable<?> callable)
    {
        expectException(callable, EOFException.class);
    }

    public static void expectException(Callable<?> callable, Class<?> exception)
    {
        boolean thrown = false;

        try
        {
            callable.call();
        }
        catch (Throwable e)
        {
            assert e.getClass().equals(exception) : e.getClass().getName() + " is not " + exception.getName();
            thrown = true;
        }

        assert thrown : exception.getName() + " not received";
    }

    public static AbstractReadCommandBuilder.SinglePartitionBuilder cmd(ColumnFamilyStore cfs, Object... partitionKey)
    {
        return new AbstractReadCommandBuilder.SinglePartitionBuilder(cfs, makeKey(cfs.metadata(), partitionKey));
    }

    public static AbstractReadCommandBuilder.PartitionRangeBuilder cmd(ColumnFamilyStore cfs)
    {
        return new AbstractReadCommandBuilder.PartitionRangeBuilder(cfs);
    }

    static DecoratedKey makeKey(TableMetadata metadata, Object... partitionKey)
    {
        if (partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey)
            return (DecoratedKey)partitionKey[0];

        ByteBuffer key = metadata.partitionKeyAsClusteringComparator().make(partitionKey).serializeAsPartitionKey();
        return metadata.partitioner.decorateKey(key);
    }

    public static void assertEmptyUnfiltered(ReadCommand command)
    {
        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(executionController))
        {
            if (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    throw new AssertionError("Expected no results for query " + command.toCQLString() + " but got key " + command.metadata().partitionKeyType.getString(partition.partitionKey().getKey()));
                }
            }
        }
    }

    public static void assertEmpty(ReadCommand command)
    {
        try (ReadExecutionController executionController = command.executionController();
             PartitionIterator iterator = command.executeInternal(executionController))
        {
            if (iterator.hasNext())
            {
                try (RowIterator partition = iterator.next())
                {
                    throw new AssertionError("Expected no results for query " + command.toCQLString() + " but got key " + command.metadata().partitionKeyType.getString(partition.partitionKey().getKey()));
                }
            }
        }
    }

    public static List<ImmutableBTreePartition> getAllUnfiltered(ReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController())
        {
            return getAllUnfiltered(command, controller);
        }
    }
    
    public static List<ImmutableBTreePartition> getAllUnfiltered(ReadCommand command, ReadExecutionController controller)
    {
        List<ImmutableBTreePartition> results = new ArrayList<>();
        try (UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    results.add(ImmutableBTreePartition.create(partition));
                }
            }
        }
        return results;
    }

    public static List<FilteredPartition> getAll(ReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController())
        {
            return getAll(command, controller);
        }
    }
    
    public static List<FilteredPartition> getAll(ReadCommand command, ReadExecutionController controller)
    {
        List<FilteredPartition> results = new ArrayList<>();
        try (PartitionIterator iterator = command.executeInternal(controller))
        {
            while (iterator.hasNext())
            {
                try (RowIterator partition = iterator.next())
                {
                    results.add(FilteredPartition.create(partition));
                }
            }
        }
        return results;
    }

    public static Row getOnlyRowUnfiltered(ReadCommand cmd)
    {
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assert iterator.hasNext() : "Expecting one row in one partition but got nothing";
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assert !iterator.hasNext() : "Expecting a single partition but got more";

                assert partition.hasNext() : "Expecting one row in one partition but got an empty partition";
                Row row = ((Row)partition.next());
                assert !partition.hasNext() : "Expecting a single row but got more";
                return row;
            }
        }
    }

    public static Row getOnlyRow(ReadCommand cmd)
    {
        try (ReadExecutionController executionController = cmd.executionController();
             PartitionIterator iterator = cmd.executeInternal(executionController))
        {
            assert iterator.hasNext() : "Expecting one row in one partition but got nothing";
            try (RowIterator partition = iterator.next())
            {
                assert !iterator.hasNext() : "Expecting a single partition but got more";
                assert partition.hasNext() : "Expecting one row in one partition but got an empty partition";
                Row row = partition.next();
                assert !partition.hasNext() : "Expecting a single row but got more";
                return row;
            }
        }
    }

    public static ImmutableBTreePartition getOnlyPartitionUnfiltered(ReadCommand cmd)
    {
        try (ReadExecutionController controller = cmd.executionController())
        {
            return getOnlyPartitionUnfiltered(cmd, controller);
        }
    }
    
    public static ImmutableBTreePartition getOnlyPartitionUnfiltered(ReadCommand cmd, ReadExecutionController controller)
    {
        try (UnfilteredPartitionIterator iterator = cmd.executeLocally(controller))
        {
            assert iterator.hasNext() : "Expecting a single partition but got nothing";
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assert !iterator.hasNext() : "Expecting a single partition but got more";
                return ImmutableBTreePartition.create(partition);
            }
        }
    }

    public static FilteredPartition getOnlyPartition(ReadCommand cmd)
    {
        return getOnlyPartition(cmd, false);
    }
    
    public static FilteredPartition getOnlyPartition(ReadCommand cmd, boolean trackRepairedStatus)
    {
        try (ReadExecutionController executionController = cmd.executionController(trackRepairedStatus);
             PartitionIterator iterator = cmd.executeInternal(executionController))
        {
            assert iterator.hasNext() : "Expecting a single partition but got nothing";
            try (RowIterator partition = iterator.next())
            {
                assert !iterator.hasNext() : "Expecting a single partition but got more";
                return FilteredPartition.create(partition);
            }
        }
    }

    public static UnfilteredRowIterator apply(Mutation mutation)
    {
        mutation.apply();
        assert mutation.getPartitionUpdates().size() == 1;
        return mutation.getPartitionUpdates().iterator().next().unfilteredIterator();
    }

    public static Cell<?> cell(ColumnFamilyStore cfs, Row row, String columnName)
    {
        ColumnMetadata def = cfs.metadata().getColumn(ByteBufferUtil.bytes(columnName));
        assert def != null;
        return row.getCell(def);
    }

    public static Row row(Partition partition, Object... clustering)
    {
        return partition.getRow(partition.metadata().comparator.make(clustering));
    }

    public static void assertCellValue(Object value, ColumnFamilyStore cfs, Row row, String columnName)
    {
        Cell<?> cell = cell(cfs, row, columnName);
        assert cell != null : "Row " + row.toString(cfs.metadata()) + " has no cell for " + columnName;
        assertEquals(value, Cells.composeValue(cell, cell.column().type));
    }

    public static void consume(UnfilteredRowIterator iter)
    {
        try (UnfilteredRowIterator iterator = iter)
        {
            while (iter.hasNext())
                iter.next();
        }
    }

    public static void consume(UnfilteredPartitionIterator iterator)
    {
        while (iterator.hasNext())
        {
            consume(iterator.next());
        }
    }

    public static int size(PartitionIterator iter)
    {
        int size = 0;
        while (iter.hasNext())
        {
            ++size;
            iter.next().close();
        }
        return size;
    }

    public static boolean equal(UnfilteredRowIterator a, UnfilteredRowIterator b)
    {
        return Objects.equals(a.columns(), b.columns())
            && Objects.equals(a.stats(), b.stats())
            && sameContent(a, b);
    }

    // Test equality of the iterators, but without caring too much about the "metadata" of said iterator. This is often
    // what we want in tests. In particular, the columns() reported by the iterators will sometimes differ because they
    // are a superset of what the iterator actually contains, and depending on the method used to get each iterator
    // tested, one may include a defined column the other don't while there is not actual content for that column.
    public static boolean sameContent(UnfilteredRowIterator a, UnfilteredRowIterator b)
    {
        return Objects.equals(a.metadata(), b.metadata())
            && Objects.equals(a.isReverseOrder(), b.isReverseOrder())
            && Objects.equals(a.partitionKey(), b.partitionKey())
            && Objects.equals(a.partitionLevelDeletion(), b.partitionLevelDeletion())
            && Objects.equals(a.staticRow(), b.staticRow())
            && Iterators.elementsEqual(a, b);
    }

    public static boolean sameContent(RowIterator a, RowIterator b)
    {
        return Objects.equals(a.metadata(), b.metadata())
               && Objects.equals(a.isReverseOrder(), b.isReverseOrder())
               && Objects.equals(a.partitionKey(), b.partitionKey())
               && Objects.equals(a.staticRow(), b.staticRow())
               && Iterators.elementsEqual(a, b);
    }

    public static boolean sameContent(Mutation a, Mutation b)
    {
        if (!a.key().equals(b.key()) || !a.getTableIds().equals(b.getTableIds()))
            return false;

        for (PartitionUpdate update : a.getPartitionUpdates())
        {
            if (!sameContent(update.unfilteredIterator(), b.getPartitionUpdate(update.metadata()).unfilteredIterator()))
                return false;
        }
        return true;
    }

    // moved & refactored from KeyspaceTest in < 3.0
    public static void assertColumns(Row row, String... expectedColumnNames)
    {
        Iterator<Cell<?>> cells = row == null ? Collections.emptyIterator() : row.cells().iterator();
        String[] actual = Iterators.toArray(Iterators.transform(cells, new Function<Cell<?>, String>()
        {
            public String apply(Cell<?> cell)
            {
                return cell.column().name.toString();
            }
        }), String.class);

        assert Arrays.equals(actual, expectedColumnNames)
        : String.format("Columns [%s])] is not expected [%s]",
                        ((row == null) ? "" : row.columns().toString()),
                        StringUtils.join(expectedColumnNames, ","));
    }

    public static void assertColumn(TableMetadata cfm, Row row, String name, String value, long timestamp)
    {
        Cell<?> cell = row.getCell(cfm.getColumn(new ColumnIdentifier(name, true)));
        assertColumn(cell, value, timestamp);
    }

    public static void assertColumn(Cell<?> cell, String value, long timestamp)
    {
        assertNotNull(cell);
        assertEquals(0, ByteBufferUtil.compareUnsigned(cell.buffer(), ByteBufferUtil.bytes(value)));
        assertEquals(timestamp, cell.timestamp());
    }

    public static void assertClustering(TableMetadata cfm, Row row, Object... clusteringValue)
    {
        assertEquals(row.clustering().size(), clusteringValue.length);
        assertEquals(0, cfm.comparator.compare(row.clustering(), cfm.comparator.make(clusteringValue)));
    }

    public static PartitionerSwitcher switchPartitioner(IPartitioner p)
    {
        return new PartitionerSwitcher(p);
    }

    public static class PartitionerSwitcher implements AutoCloseable
    {
        final IPartitioner oldP;
        final IPartitioner newP;

        public PartitionerSwitcher(IPartitioner partitioner)
        {
            newP = partitioner;
            oldP = StorageService.instance.setPartitionerUnsafe(partitioner);
        }

        public void close()
        {
            IPartitioner p = StorageService.instance.setPartitionerUnsafe(oldP);
            assert p == newP;
        }
    }

    public static void spinAssertEquals(Object expected, Supplier<Object> actualSupplier, int timeoutInSeconds)
    {
        spinAssertEquals(null, expected, actualSupplier, timeoutInSeconds, TimeUnit.SECONDS);
    }

    public static <T> void spinAssertEquals(String message, T expected, Supplier<? extends T> actualSupplier, long timeout, TimeUnit timeUnit)
    {
        Awaitility.await()
                  .pollInterval(Duration.ofMillis(100))
                  .pollDelay(0, TimeUnit.MILLISECONDS)
                  .atMost(timeout, timeUnit)
                  .untilAsserted(() -> assertThat(message, actualSupplier.get(), equalTo(expected)));
    }

    public static void joinThread(Thread thread) throws InterruptedException
    {
        thread.join(10000);
    }

    public static AssertionError runCatchingAssertionError(Runnable test)
    {
        try
        {
            test.run();
            return null;
        }
        catch (AssertionError e)
        {
            return e;
        }
    }

    /**
     * Wrapper function used to run a test that can sometimes flake for uncontrollable reasons.
     *
     * If the given test fails on the first run, it is executed the given number of times again, expecting all secondary
     * runs to succeed. If they do, the failure is understood as a flake and the test is treated as passing.
     *
     * Do not use this if the test is deterministic and its success is not influenced by external factors (such as time,
     * selection of random seed, network failures, etc.). If the test can be made independent of such factors, it is
     * probably preferable to do so rather than use this method.
     *
     * @param test The test to run.
     * @param rerunsOnFailure How many times to re-run it if it fails. All reruns must pass.
     * @param message Message to send to System.err on initial failure.
     */
    public static void flakyTest(Runnable test, int rerunsOnFailure, String message)
    {
        AssertionError e = runCatchingAssertionError(test);
        if (e == null)
            return;     // success

        logger.info("Test failed. {}", message, e);
        logger.info("Re-running {} times to verify it isn't failing more often than it should.", rerunsOnFailure);

        int rerunsFailed = 0;
        for (int i = 0; i < rerunsOnFailure; ++i)
        {
            AssertionError t = runCatchingAssertionError(test);
            if (t != null)
            {
                ++rerunsFailed;
                e.addSuppressed(t);

                logger.debug("Test failed again, total num failures: {}", rerunsFailed, t);
            }
        }
        if (rerunsFailed > 0)
        {
            logger.error("Test failed in {} of the {} reruns.", rerunsFailed, rerunsOnFailure);
            throw e;
        }

        logger.info("All reruns succeeded. Failure treated as flake.");
    }

    // for use with Optional in tests, can be used as an argument to orElseThrow
    public static Supplier<AssertionError> throwAssert(final String message)
    {
        return () -> new AssertionError(message);
    }

    public static class UnfilteredSource extends AbstractUnfilteredRowIterator implements UnfilteredRowIterator
    {
        Iterator<Unfiltered> content;

        public UnfilteredSource(TableMetadata metadata, DecoratedKey partitionKey, Row staticRow, Iterator<Unfiltered> content)
        {
            super(metadata,
                  partitionKey,
                  DeletionTime.LIVE,
                  metadata.regularAndStaticColumns(),
                  staticRow != null ? staticRow : Rows.EMPTY_STATIC_ROW,
                  false,
                  EncodingStats.NO_STATS);
            this.content = content;
        }

        @Override
        protected Unfiltered computeNext()
        {
            return content.hasNext() ? content.next() : endOfData();
        }
    }

    public static UnfilteredPartitionIterator executeLocally(PartitionRangeReadCommand command,
                                                             ColumnFamilyStore cfs,
                                                             ReadExecutionController controller)
    {
        return command.queryStorage(cfs, controller);
    }

    public static Closeable markDirectoriesUnwriteable(ColumnFamilyStore cfs)
    {
        try
        {
            for ( ; ; )
            {
                DataDirectory dir = cfs.getDirectories().getWriteableLocation(1);
                DisallowedDirectories.maybeMarkUnwritable(cfs.getDirectories().getLocationForDisk(dir));
            }
        }
        catch (IOError e)
        {
            // Expected -- marked all directories as unwritable
        }
        return () -> DisallowedDirectories.clearUnwritableUnsafe();
    }

    public static PagingState makeSomePagingState(ProtocolVersion protocolVersion)
    {
        return makeSomePagingState(protocolVersion, Integer.MAX_VALUE);
    }

    public static PagingState makeSomePagingState(ProtocolVersion protocolVersion, int remainingInPartition)
    {
        TableMetadata metadata =
            TableMetadata.builder("ks", "tbl")
                         .addPartitionKeyColumn("k", AsciiType.instance)
                         .addClusteringColumn("c1", AsciiType.instance)
                         .addClusteringColumn("c2", Int32Type.instance)
                         .addRegularColumn("myCol", AsciiType.instance)
                         .build();

        ByteBuffer pk = ByteBufferUtil.bytes("someKey");

        ColumnMetadata def = metadata.getColumn(new ColumnIdentifier("myCol", false));
        Clustering<?> c = Clustering.make(ByteBufferUtil.bytes("c1"), ByteBufferUtil.bytes(42));
        Row row = BTreeRow.singleCellRow(c, BufferCell.live(def, 0, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        PagingState.RowMark mark = PagingState.RowMark.create(metadata, row, protocolVersion);
        return new PagingState(pk, mark, 10, remainingInPartition);
    }

    public static void assertRCEquals(ReplicaCollection<?> a, ReplicaCollection<?> b)
    {
        assertTrue(a + " not equal to " + b, Iterables.elementsEqual(a, b));
    }

    public static void assertNotRCEquals(ReplicaCollection<?> a, ReplicaCollection<?> b)
    {
        assertFalse(a + " equal to " + b, Iterables.elementsEqual(a, b));
    }

    /**
     * Makes sure that the sstables on disk are the same ones as the cfs live sstables (that they have the same generation)
     */
    public static void assertOnDiskState(ColumnFamilyStore cfs, int expectedSSTableCount)
    {
        LifecycleTransaction.waitForDeletions();
        assertEquals(expectedSSTableCount, cfs.getLiveSSTables().size());
        Set<SSTableId> liveIdentifiers = cfs.getLiveSSTables().stream()
                                            .map(sstable -> sstable.descriptor.id)
                                            .collect(Collectors.toSet());
        int fileCount = 0;
        for (File f : cfs.getDirectories().getCFDirectories())
        {
            for (File sst : f.tryList())
            {
                if (sst.name().contains("Data"))
                {
                    Descriptor d = Descriptor.fromFilename(sst.absolutePath());
                    assertTrue(liveIdentifiers.contains(d.id));
                    fileCount++;
                }
            }
        }
        assertEquals(expectedSSTableCount, fileCount);
    }

    /**
     * Disable bloom filter on all sstables of given table
     */
    public static void disableBloomFilter(ColumnFamilyStore cfs)
    {
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            for (SSTableReader sstable : sstables)
            {
                sstable = sstable.cloneAndReplace(FilterFactory.AlwaysPresent);
                txn.update(sstable, true);
                txn.checkpoint();
            }
            txn.finish();
        }

        for (SSTableReader reader : cfs.getLiveSSTables())
            assertEquals(FilterFactory.AlwaysPresent, reader.getBloomFilter());
    }

    /**
     * Setups Gossiper to mimic the upgrade behaviour when {@link Gossiper#isUpgradingFromVersionLowerThan(CassandraVersion)}
     * or {@link Gossiper#hasMajorVersion3Nodes()} is called.
     */
    public static void setUpgradeFromVersion(String version)
    {
        int v = Optional.ofNullable(Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddressAndPort()))
                        .map(ep -> ep.getApplicationState(ApplicationState.RELEASE_VERSION))
                        .map(rv -> rv.version)
                        .orElse(0);

        Gossiper.instance.addLocalApplicationState(ApplicationState.RELEASE_VERSION,
                                                   VersionedValue.unsafeMakeVersionedValue(version, v + 1));
        try
        {
            // add dummy host to avoid returning early in Gossiper.instance.upgradeFromVersionSupplier
            Gossiper.instance.initializeNodeUnsafe(InetAddressAndPort.getByName("127.0.0.2"), UUID.randomUUID(), 1);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        Gossiper.instance.expireUpgradeFromVersion();
    }

    /**
     * Sets the length of the file to given size. File will be created if not exist.
     *
     * @param file file for which length needs to be set
     * @param size new szie
     * @throws IOException on any I/O error.
     */
    public static void setFileLength(File file, long size) throws IOException
    {
        try (FileChannel fileChannel = file.newReadWriteChannel())
        {
            if (file.length() >= size)
            {
                fileChannel.truncate(size);
            }
            else
            {
                fileChannel.position(size - 1);
                fileChannel.write(ByteBuffer.wrap(new byte[1]));
            }
        }
    }

    public static Supplier<SequenceBasedSSTableId> newSeqGen(int ... existing)
    {
        return SequenceBasedSSTableId.Builder.instance.generator(IntStream.of(existing).mapToObj(SequenceBasedSSTableId::new));
    }

    public static Supplier<UUIDBasedSSTableId> newUUIDGen()
    {
        return UUIDBasedSSTableId.Builder.instance.generator(Stream.empty());
    }

    public static Set<Descriptor> getSSTables(String ks, String tableName)
    {
        return Keyspace.open(ks)
                       .getColumnFamilyStore(tableName)
                       .getLiveSSTables()
                       .stream()
                       .map(sstr -> sstr.descriptor)
                       .collect(Collectors.toSet());
    }

    public static Set<Descriptor> getSnapshots(String ks, String tableName, String snapshotTag)
    {
        try
        {
            return Keyspace.open(ks)
                           .getColumnFamilyStore(tableName)
                           .getSnapshotSSTableReaders(snapshotTag)
                           .stream()
                           .map(sstr -> sstr.descriptor)
                           .collect(Collectors.toSet());
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    public static Set<Descriptor> getBackups(String ks, String tableName)
    {
        return Keyspace.open(ks)
                       .getColumnFamilyStore(tableName)
                       .getDirectories()
                       .sstableLister(Directories.OnTxnErr.THROW)
                       .onlyBackups(true)
                       .list()
                       .keySet();
    }

    public static StreamState bulkLoadSSTables(File dir, String targetKeyspace)
    {
        SSTableLoader.Client client = new SSTableLoader.Client()
        {
            private String keyspace;

            public void init(String keyspace)
            {
                this.keyspace = keyspace;
                for (Replica replica : StorageService.instance.getLocalReplicas(keyspace))
                    addRangeForEndpoint(replica.range(), FBUtilities.getBroadcastAddressAndPort());
            }

            public TableMetadataRef getTableMetadata(String tableName)
            {
                return Schema.instance.getTableMetadataRef(keyspace, tableName);
            }
        };

        SSTableLoader loader = new SSTableLoader(dir, client, new OutputHandler.LogOutput(), 1, targetKeyspace);
        StreamResultFuture result = loader.stream();
        return FBUtilities.waitOnFuture(result);
    }

    public static File relativizePath(File targetBasePath, File path, int components)
    {
        Preconditions.checkArgument(components > 0);
        Preconditions.checkArgument(path.toPath().getNameCount() >= components);
        Path relative = path.toPath().subpath(path.toPath().getNameCount() - components, path.toPath().getNameCount());
        return new File(targetBasePath.toPath().resolve(relative));
    }

    public static void flush(ColumnFamilyStore cfs)
    {
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    public static void flushTable(Keyspace keyspace, String table)
    {
        flush(keyspace.getColumnFamilyStore(table));
    }

    public static void flushTable(Keyspace keyspace, TableId table)
    {
        flush(keyspace.getColumnFamilyStore(table));
    }

    public static void flushTable(String keyspace, String table)
    {
        flushTable(Keyspace.open(keyspace), table);
    }

    public static void flush(Keyspace keyspace)
    {
        FBUtilities.waitOnFutures(keyspace.flush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
    }

    public static void flushKeyspace(String keyspaceName)
    {
        flush(Keyspace.open(keyspaceName));
    }

    public static void flush(TableViews view)
    {
        view.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }
}
