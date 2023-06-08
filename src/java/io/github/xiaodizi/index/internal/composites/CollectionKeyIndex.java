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
package io.github.xiaodizi.index.internal.composites;

import java.nio.ByteBuffer;

import io.github.xiaodizi.schema.ColumnMetadata;
import io.github.xiaodizi.cql3.Operator;
import io.github.xiaodizi.db.Clustering;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.marshal.SetType;
import io.github.xiaodizi.db.rows.Cell;
import io.github.xiaodizi.db.rows.CellPath;
import io.github.xiaodizi.db.rows.Row;
import io.github.xiaodizi.schema.IndexMetadata;

/**
 * Index on the collection element of the cell name of a collection.
 *
 * The row keys for this index are given by the collection element for
 * indexed columns.
 */
public class CollectionKeyIndex extends CollectionKeyIndexBase
{
    public CollectionKeyIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        super(baseCfs, indexDef);
    }

    public ByteBuffer getIndexedValue(ByteBuffer partitionKey,
                                      Clustering<?> clustering,
                                      CellPath path,
                                      ByteBuffer cellValue)
    {
        return path.get(0);
    }

    public boolean isStale(Row data, ByteBuffer indexValue, int nowInSec)
    {
        Cell<?> cell = data.getCell(indexedColumn, CellPath.create(indexValue));
        return cell == null || !cell.isLive(nowInSec);
    }

    public boolean supportsOperator(ColumnMetadata indexedColumn, Operator operator)
    {
        return operator == Operator.CONTAINS_KEY ||
               operator == Operator.CONTAINS && indexedColumn.type instanceof SetType;
    }
}
