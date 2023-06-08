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

package io.github.xiaodizi.db;

import io.github.xiaodizi.db.filter.ClusteringIndexFilter;
import io.github.xiaodizi.db.filter.ColumnFilter;
import io.github.xiaodizi.db.partitions.PartitionUpdate;
import io.github.xiaodizi.db.rows.UnfilteredRowIterator;
import io.github.xiaodizi.db.rows.UnfilteredRowIteratorWithLowerBound;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.io.sstable.format.SSTableReadsListener;
import io.github.xiaodizi.schema.TableId;
import io.github.xiaodizi.utils.FBUtilities;

public interface StorageHook
{
    public static final StorageHook instance = createHook();

    public void reportWrite(TableId tableId, PartitionUpdate partitionUpdate);
    public void reportRead(TableId tableId, DecoratedKey key);
    public UnfilteredRowIteratorWithLowerBound makeRowIteratorWithLowerBound(ColumnFamilyStore cfs,
                                                                      DecoratedKey partitionKey,
                                                                      SSTableReader sstable,
                                                                      ClusteringIndexFilter filter,
                                                                      ColumnFilter selectedColumns,
                                                                      SSTableReadsListener listener);
    public UnfilteredRowIterator makeRowIterator(ColumnFamilyStore cfs,
                                                 SSTableReader sstable,
                                                 DecoratedKey key,
                                                 Slices slices,
                                                 ColumnFilter selectedColumns,
                                                 boolean reversed,
                                                 SSTableReadsListener listener);

    static StorageHook createHook()
    {
        String className =  System.getProperty("cassandra.storage_hook");
        if (className != null)
        {
            return FBUtilities.construct(className, StorageHook.class.getSimpleName());
        }

        return new StorageHook()
        {
            public void reportWrite(TableId tableId, PartitionUpdate partitionUpdate) {}

            public void reportRead(TableId tableId, DecoratedKey key) {}

            public UnfilteredRowIteratorWithLowerBound makeRowIteratorWithLowerBound(ColumnFamilyStore cfs,
                                                                                     DecoratedKey partitionKey,
                                                                                     SSTableReader sstable,
                                                                                     ClusteringIndexFilter filter,
                                                                                     ColumnFilter selectedColumns,
                                                                                     SSTableReadsListener listener)
            {
                return new UnfilteredRowIteratorWithLowerBound(partitionKey,
                                                               sstable,
                                                               filter,
                                                               selectedColumns,
                                                               listener);
            }

            public UnfilteredRowIterator makeRowIterator(ColumnFamilyStore cfs,
                                                         SSTableReader sstable,
                                                         DecoratedKey key,
                                                         Slices slices,
                                                         ColumnFilter selectedColumns,
                                                         boolean reversed,
                                                         SSTableReadsListener listener)
            {
                return sstable.rowIterator(key, slices, selectedColumns, reversed, listener);
            }
        };
    }
}
