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

package io.github.xiaodizi.db.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import io.github.xiaodizi.cql3.ColumnIdentifier;
import io.github.xiaodizi.cql3.Operator;
import io.github.xiaodizi.db.Clustering;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.DeletionTime;
import io.github.xiaodizi.db.LivenessInfo;
import io.github.xiaodizi.db.RegularAndStaticColumns;
import io.github.xiaodizi.db.marshal.Int32Type;
import io.github.xiaodizi.db.partitions.SingletonUnfilteredPartitionIterator;
import io.github.xiaodizi.db.partitions.UnfilteredPartitionIterator;
import io.github.xiaodizi.db.rows.BTreeRow;
import io.github.xiaodizi.db.rows.BufferCell;
import io.github.xiaodizi.db.rows.Cell;
import io.github.xiaodizi.db.rows.EncodingStats;
import io.github.xiaodizi.db.rows.Row;
import io.github.xiaodizi.db.rows.Rows;
import io.github.xiaodizi.db.rows.Unfiltered;
import io.github.xiaodizi.db.rows.UnfilteredRowIterator;
import io.github.xiaodizi.schema.ColumnMetadata;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.utils.btree.BTree;

public class RowFilterTest
{

    @Test
    public void testCQLFilterClose()
    {
        // CASSANDRA-15126
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));

        ByteBuffer one = Int32Type.instance.decompose(1);
        RowFilter filter = RowFilter.NONE.withNewExpressions(new ArrayList<>());
        filter.add(s, Operator.NEQ, one);
        AtomicBoolean closed = new AtomicBoolean();
        UnfilteredPartitionIterator iter = filter.filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public boolean hasNext() { return false; }
            public Unfiltered next() { return null; }
            public Row staticRow()
            {
                return BTreeRow.create(Clustering.STATIC_CLUSTERING,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(s, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        Assert.assertFalse(iter.hasNext());
        Assert.assertTrue(closed.get());

        filter = RowFilter.NONE.withNewExpressions(new ArrayList<>());
        filter.add(r, Operator.NEQ, one);
        closed.set(false);
        iter = filter.filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            boolean hasNext = true;
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public boolean hasNext()
            {
                boolean r = hasNext;
                hasNext = false;
                return r;
            }
            public Unfiltered next()
            {
                return BTreeRow.create(Clustering.EMPTY,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(r, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        Assert.assertFalse(iter.hasNext());
        Assert.assertTrue(closed.get());
    }


}
