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

package io.github.xiaodizi.db.streaming;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import io.github.xiaodizi.SchemaLoader;
import io.github.xiaodizi.Util;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.RowUpdateBuilder;
import io.github.xiaodizi.db.compaction.CompactionManager;
import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.io.sstable.KeyIterator;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.schema.CachingParams;
import io.github.xiaodizi.schema.KeyspaceParams;
import io.github.xiaodizi.streaming.StreamOperation;
import io.github.xiaodizi.utils.ByteBufferUtil;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CassandraOutgoingFileTest
{
    public static final String KEYSPACE = "CassandraOutgoingFileTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_INDEXED = "Indexed1";
    public static final String CF_STANDARDLOWINDEXINTERVAL = "StandardLowIndexInterval";

    private static SSTableReader sstable;
    private static ColumnFamilyStore store;

    @BeforeClass
    public static void defineSchemaAndPrepareSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE, CF_INDEXED, true),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARDLOWINDEXINTERVAL)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(256)
                                                .caching(CachingParams.CACHE_NOTHING));

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        store = keyspace.getColumnFamilyStore(CF_STANDARD);

        // insert data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        sstable = store.getLiveSSTables().iterator().next();
    }

    @Test
    public void validateFullyContainedIn_SingleContiguousRange_Succeeds()
    {
        List<Range<Token>> requestedRanges = Arrays.asList(new Range<>(store.getPartitioner().getMinimumToken(), sstable.last.getToken()));

        List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(requestedRanges);
        CassandraOutgoingFile cof = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(),
                                                              sections,
                                                              requestedRanges, sstable.estimatedKeys());

        assertTrue(cof.contained(sections, sstable));
    }

    @Test
    public void validateFullyContainedIn_PartialOverlap_Fails()
    {
        List<Range<Token>> requestedRanges = Arrays.asList(new Range<>(store.getPartitioner().getMinimumToken(), getTokenAtIndex(2)));

        List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(requestedRanges);
        CassandraOutgoingFile cof = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(),
                                                              sections,
                                                              requestedRanges, sstable.estimatedKeys());

        assertFalse(cof.contained(sections, sstable));
    }

    @Test
    public void validateFullyContainedIn_SplitRange_Succeeds()
    {
        List<Range<Token>> requestedRanges = Arrays.asList(new Range<>(store.getPartitioner().getMinimumToken(), getTokenAtIndex(4)),
                                                         new Range<>(getTokenAtIndex(2), getTokenAtIndex(6)),
                                                         new Range<>(getTokenAtIndex(5), sstable.last.getToken()));
        requestedRanges = Range.normalize(requestedRanges);

        List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(requestedRanges);
        CassandraOutgoingFile cof = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(),
                                                              sections,
                                                              requestedRanges, sstable.estimatedKeys());

        assertTrue(cof.contained(sections, sstable));
    }

    private DecoratedKey getKeyAtIndex(int i)
    {
        int count = 0;
        DecoratedKey key;

        try (KeyIterator iter = new KeyIterator(sstable.descriptor, sstable.metadata()))
        {
            do
            {
                key = iter.next();
                count++;
            } while (iter.hasNext() && count < i);
        }
        return key;
    }

    private Token getTokenAtIndex(int i)
    {
        return getKeyAtIndex(i).getToken();
    }
}
