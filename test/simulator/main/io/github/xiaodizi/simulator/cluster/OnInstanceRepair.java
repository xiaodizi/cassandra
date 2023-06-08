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

package io.github.xiaodizi.simulator.cluster;

import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.distributed.api.IIsolatedExecutor;
import io.github.xiaodizi.locator.Replica;
import io.github.xiaodizi.locator.TokenMetadata;
import io.github.xiaodizi.repair.RepairParallelism;
import io.github.xiaodizi.repair.messages.RepairOption;
import io.github.xiaodizi.service.StorageService;
import io.github.xiaodizi.streaming.PreviewKind;
import io.github.xiaodizi.utils.concurrent.Condition;
import io.github.xiaodizi.utils.progress.ProgressEventType;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static io.github.xiaodizi.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static io.github.xiaodizi.simulator.cluster.Utils.currentToken;
import static io.github.xiaodizi.simulator.cluster.Utils.parseTokenRanges;
import static io.github.xiaodizi.utils.FBUtilities.getBroadcastAddressAndPort;
import static io.github.xiaodizi.utils.concurrent.Condition.newOneTimeCondition;
import static java.util.Collections.singletonList;

class OnInstanceRepair extends ClusterAction
{
    public OnInstanceRepair(KeyspaceActions actions, int on, boolean repairPaxos, boolean repairOnlyPaxos, boolean force)
    {
        super("Repair on " + on, RELIABLE_NO_TIMEOUTS, RELIABLE_NO_TIMEOUTS, actions, on, invokableBlockingRepair(actions.keyspace, repairPaxos, repairOnlyPaxos, false, force));
    }

    public OnInstanceRepair(KeyspaceActions actions, int on, boolean repairPaxos, boolean repairOnlyPaxos, Map.Entry<String, String> repairRange, boolean force)
    {
        this(actions, on, RELIABLE_NO_TIMEOUTS, RELIABLE_NO_TIMEOUTS, repairPaxos, repairOnlyPaxos, repairRange, force);
    }

    public OnInstanceRepair(KeyspaceActions actions, int on, Modifiers self, Modifiers transitive, String id, boolean repairPaxos, boolean repairOnlyPaxos, boolean primaryRangeOnly, boolean force)
    {
        super(id, self, transitive, actions, on, invokableBlockingRepair(actions.keyspace, repairPaxos, repairOnlyPaxos, primaryRangeOnly, force));
    }

    public OnInstanceRepair(KeyspaceActions actions, int on, Modifiers self, Modifiers transitive, boolean repairPaxos, boolean repairOnlyPaxos, Map.Entry<String, String> repairRange, boolean force)
    {
        super("Repair on " + on, self, transitive, actions, on, invokableBlockingRepair(actions.keyspace, repairPaxos, repairOnlyPaxos, repairRange, force));
    }

    private static IIsolatedExecutor.SerializableRunnable invokableBlockingRepair(String keyspaceName, boolean repairPaxos, boolean repairOnlyPaxos, boolean primaryRangeOnly, boolean force)
    {
        return () -> {
            Condition done = newOneTimeCondition();
            invokeRepair(keyspaceName, repairPaxos, repairOnlyPaxos, primaryRangeOnly, force, done::signal);
            done.awaitThrowUncheckedOnInterrupt();
        };
    }

    private static IIsolatedExecutor.SerializableRunnable invokableBlockingRepair(String keyspaceName, boolean repairPaxos, boolean repairOnlyPaxos, Map.Entry<String, String> repairRange, boolean force)
    {
        return () -> {
            Condition done = newOneTimeCondition();
            invokeRepair(keyspaceName, repairPaxos, repairOnlyPaxos, () -> parseTokenRanges(singletonList(repairRange)), false, force, done::signal);
            done.awaitThrowUncheckedOnInterrupt();
        };
    }

    private static void invokeRepair(String keyspaceName, boolean repairPaxos, boolean repairOnlyPaxos, boolean primaryRangeOnly, boolean force, Runnable listener)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        TokenMetadata metadata = StorageService.instance.getTokenMetadata().cloneOnlyTokenMap();
        invokeRepair(keyspaceName, repairPaxos, repairOnlyPaxos,
                     () -> primaryRangeOnly ? Collections.singletonList(metadata.getPrimaryRangeFor(currentToken()))
                                            : keyspace.getReplicationStrategy().getAddressReplicas(metadata).get(getBroadcastAddressAndPort()).asList(Replica::range),
                     primaryRangeOnly, force, listener);
    }

    private static void invokeRepair(String keyspaceName, boolean repairPaxos, boolean repairOnlyPaxos, IIsolatedExecutor.SerializableCallable<Collection<Range<Token>>> rangesSupplier, boolean isPrimaryRangeOnly, boolean force, Runnable listener)
    {
        Collection<Range<Token>> ranges = rangesSupplier.call();
        // no need to wait for completion, as we track all task submissions and message exchanges, and ensure they finish before continuing to next action
        StorageService.instance.repair(keyspaceName, new RepairOption(RepairParallelism.SEQUENTIAL, isPrimaryRangeOnly, false, false, 1, ranges, false, false, force, PreviewKind.NONE, false, true, repairPaxos, repairOnlyPaxos), singletonList((tag, event) -> {
            if (event.getType() == ProgressEventType.COMPLETE)
                listener.run();
        }));
    }

}
