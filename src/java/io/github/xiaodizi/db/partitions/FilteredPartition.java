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
package io.github.xiaodizi.db.partitions;

import java.util.Iterator;

import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.DeletionInfo;
import io.github.xiaodizi.db.RegularAndStaticColumns;
import io.github.xiaodizi.db.rows.*;

public class FilteredPartition extends ImmutableBTreePartition
{
    public FilteredPartition(RowIterator rows)
    {
        super(rows.metadata(), rows.partitionKey(), build(rows, DeletionInfo.LIVE, false));
    }

    /**
     * Create a FilteredPartition holding all the rows of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     */
    public static FilteredPartition create(RowIterator iterator)
    {
        return new FilteredPartition(iterator);
    }

    public RowIterator rowIterator()
    {
        final Iterator<Row> iter = iterator();
        return new RowIterator()
        {
            public TableMetadata metadata()
            {
                return FilteredPartition.this.metadata();
            }

            public boolean isReverseOrder()
            {
                return false;
            }

            public RegularAndStaticColumns columns()
            {
                return FilteredPartition.this.columns();
            }

            public DecoratedKey partitionKey()
            {
                return FilteredPartition.this.partitionKey();
            }

            public Row staticRow()
            {
                return FilteredPartition.this.staticRow();
            }

            public void close() {}

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Row next()
            {
                return iter.next();
            }

            public boolean isEmpty()
            {
                return staticRow().isEmpty() && !hasRows();
            }
        };
    }
}
