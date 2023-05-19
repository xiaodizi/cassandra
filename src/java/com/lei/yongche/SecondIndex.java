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

package com.lei.yongche;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

public class SecondIndex implements Index, INotificationConsumer {

    private final ColumnFamilyStore baseCfs;
    private final IndexMetadata config;
    private final ColumnIndex index;

    public SecondIndex(ColumnFamilyStore baseCfs, IndexMetadata config){
        this.baseCfs = baseCfs;
        this.config = config;
        System.out.println("----------lei------------");
        System.out.println(baseCfs);
        System.out.println(config);
        System.out.println("-------------------------");
        ColumnMetadata column = TargetParser.parse(baseCfs.metadata(), config).left;
        this.index = new ColumnIndex(baseCfs.metadata().partitionKeyType, column, config);
    }
    @Override
    public Callable<?> getInitializationTask() {
        System.out.println("------------lei test1----------");
        return null;
    }

    @Override
    public IndexMetadata getIndexMetadata() {
        System.out.println("------------lei test2----------");
        return config;
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) {
        System.out.println("------------lei test3----------");
        return null;
    }

    @Override
    public void register(IndexRegistry registry) {
        System.out.println("------------lei test4----------");
        registry.registerIndex(this);
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable() {
        System.out.println("------------lei test5----------");
        return Optional.empty();
    }

    @Override
    public Callable<?> getBlockingFlushTask() {
        System.out.println("------------lei test6----------");
        return null;
    }

    @Override
    public Callable<?> getInvalidateTask() {
        System.out.println("------------lei test7----------");
        return null;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt) {
        System.out.println("------------lei test8----------");
        return null;
    }

    @Override
    public boolean shouldBuildBlocking() {
        System.out.println("------------lei test9----------");
        return false;
    }

    @Override
    public boolean dependsOn(ColumnMetadata column) {
        System.out.println("------------lei test10----------");
        return false;
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator) {
        System.out.println("------------lei test11----------");
        return false;
    }

    @Override
    public AbstractType<?> customExpressionValueType() {
        System.out.println("------------lei test12----------");
        return null;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter) {
        System.out.println("------------lei test13----------");
        return null;
    }

    @Override
    public long getEstimatedResultRows() {
        System.out.println("------------lei test14----------");
        return 0;
    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException {
        System.out.println("------------lei test15----------");
    }

    @Override
    public Indexer indexerFor(DecoratedKey key, RegularAndStaticColumns columns, int nowInSec, WriteContext ctx, IndexTransaction.Type transactionType) {
        System.out.println("------------lei test16----------");
        return null;
    }

    @Override
    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command) {
        System.out.println("------------lei test17----------");
        return null;
    }

    @Override
    public Searcher searcherFor(ReadCommand command) {
        System.out.println("------------lei test18----------");
        return null;
    }

    @Override
    public void handleNotification(INotification notification, Object sender) {
        System.out.println("------------lei test----------");
    }
}
