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

package io.github.xiaodizi.service.reads.repair;

import java.util.Map;
import java.util.function.Consumer;

import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.Mutation;
import io.github.xiaodizi.db.partitions.PartitionIterator;
import io.github.xiaodizi.db.partitions.UnfilteredPartitionIterators;
import io.github.xiaodizi.exceptions.ReadTimeoutException;
import io.github.xiaodizi.locator.Endpoints;
import io.github.xiaodizi.locator.Replica;
import io.github.xiaodizi.locator.ReplicaPlan;
import io.github.xiaodizi.service.reads.DigestResolver;

/**
 * Bypasses the read repair path for short read protection and testing
 */
public class NoopReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> implements ReadRepair<E, P>
{
    public static final NoopReadRepair instance = new NoopReadRepair();

    private NoopReadRepair() {}

    @Override
    public UnfilteredPartitionIterators.MergeListener getMergeListener(P replicas)
    {
        return UnfilteredPartitionIterators.MergeListener.NOOP;
    }

    @Override
    public void startRepair(DigestResolver<E, P> digestResolver, Consumer<PartitionIterator> resultConsumer)
    {
        resultConsumer.accept(digestResolver.getData());
    }

    public void awaitReads() throws ReadTimeoutException
    {
    }

    @Override
    public void maybeSendAdditionalReads()
    {

    }

    @Override
    public void maybeSendAdditionalWrites()
    {

    }

    @Override
    public void awaitWrites()
    {

    }

    @Override
    public void repairPartition(DecoratedKey partitionKey, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan)
    {

    }
}
