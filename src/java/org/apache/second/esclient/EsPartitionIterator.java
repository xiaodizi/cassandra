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

package org.apache.second.esclient;

import com.alibaba.fastjson2.JSONObject;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.second.ElasticSecondaryIndex;

import java.util.*;


public class EsPartitionIterator implements UnfilteredPartitionIterator {


    private static final String FAKE_METADATA = "{\"metadata\":\"none\"}";

    private static final String ES_HITS = "hits";
    private static final String ES_SOURCE = "_source";

    private final Iterator<SearchResultRow> esResultIterator;
    private final ColumnFamilyStore baseCfs;
    private final ReadCommand command;

    private final ElasticSecondaryIndex index;

    private final List<String> partitionKeysNames;


    public EsPartitionIterator(ElasticSecondaryIndex index, SearchResult searchResult, List<String> partitionKeysNames, ReadCommand command, String searchId) {
        this.baseCfs = index.baseCfs;
        this.esResultIterator = searchResult.items.iterator();
        this.command = command;
        this.index = index;
        this.partitionKeysNames = partitionKeysNames;
        Tracing.trace("ESI {} FakePartitionIterator initialized", searchId);
    }

    @Override
    public void close() {

    }

    @Override
    public TableMetadata metadata() {
        return command.metadata();
    }

    @Override
    public boolean hasNext() {
        return esResultIterator.hasNext();
    }

    @Override
    public UnfilteredRowIterator next() {

        if (!esResultIterator.hasNext()) {
            return null;
        }



        //Build the minimum row
        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        rowBuilder.newRow(Clustering.EMPTY);
        rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.EMPTY);
        rowBuilder.addRowDeletion(Row.Deletion.LIVE);

        SearchResultRow esResult = esResultIterator.next();

        JSONObject jsonMetadata = esResult.docMetadata;

        //And PK value
        DecoratedKey partitionKey = baseCfs.getPartitioner().decorateKey(esResult.partitionKey);

        SinglePartitionReadCommand readCommand = SinglePartitionReadCommand.create(
                this.baseCfs.metadata(),
                command.nowInSec(),
                command.columnFilter(),
                RowFilter.NONE,
                DataLimits.NONE,
                partitionKey,
                command.clusteringIndexFilter(partitionKey));

        PartitionIterator partition =
                StorageProxy.read(SinglePartitionReadCommand.Group.one(readCommand), ConsistencyLevel.ALL, System.nanoTime());

        Row next = partition.next().next();


        System.out.println("继续执行");
        return new SingleRowIterator(this.baseCfs.metadata(), next, partitionKey, this.baseCfs.metadata().regularAndStaticColumns());
    }
}
