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

package org.apache.second;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.nio.charset.CharacterCodingException;


public class ReadCommandPostProcessor extends IndexPostProcessor {

    private final ColumnFamilyStore baseCfs;
    private final IndexMetadata config;

    private static String commondSchema;

    private final OpenSearchClient client;

    private final String indexName;


    public ReadCommandPostProcessor(OpenSearchClient client, String indexName, ColumnFamilyStore baseCfs, IndexMetadata config) {
        this.client = client;
        this.baseCfs = baseCfs;
        this.config = config;
        this.indexName = indexName;
    }

    @Override
    public PartitionIterator apply(PartitionIterator partitionIterator, ReadCommand readCommand) {

        // 构造一个新的 PartitionIterator 对象，如果没有再返回本身的。
        RowIterator next = partitionIterator.next();

        EsRowIterator esRowIterator = new EsRowIterator(next, next);

        return new EsPartitionIterator(esRowIterator);
    }
}
