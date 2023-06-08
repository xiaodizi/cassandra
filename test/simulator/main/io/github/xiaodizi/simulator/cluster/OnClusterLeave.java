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

import io.github.xiaodizi.distributed.api.IInvokableInstance;
import io.github.xiaodizi.service.StorageService;
import io.github.xiaodizi.simulator.ActionList;
import io.github.xiaodizi.simulator.systems.SimulatedActionConsumer;
import io.github.xiaodizi.utils.concurrent.Future;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.github.xiaodizi.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static io.github.xiaodizi.utils.LazyToString.lazy;

class OnClusterLeave extends OnClusterChangeTopology
{
    final int leaving;

    OnClusterLeave(KeyspaceActions actions, Topology before, Topology during, Topology after, int leaving)
    {
        super(lazy(() -> String.format("node%d Leaving", leaving)), actions, before, after, during.pendingKeys());
        this.leaving = leaving;
    }

    public ActionList performSimple()
    {
        IInvokableInstance leaveInstance = actions.cluster.get(leaving);
        before(leaveInstance);
        AtomicReference<Supplier<? extends Future<?>>> preparedUnbootstrap = new AtomicReference<>();
        return ActionList.of(
            // setup the node's own gossip state for pending ownership, and return gossip actions to disseminate
            new OnClusterUpdateGossip(actions, leaving, new OnInstanceSetLeaving(actions, leaving)),
            new SimulatedActionConsumer<>("Prepare unbootstrap on " + leaving, RELIABLE_NO_TIMEOUTS, RELIABLE_NO_TIMEOUTS, actions, leaveInstance,
                                          ref -> ref.set(StorageService.instance.prepareUnbootstrapStreaming()), preparedUnbootstrap),
            new OnInstanceTopologyChangePaxosRepair(actions, leaving, "Leave"),
            new SimulatedActionConsumer<>("Execute unbootstrap on " + leaving, RELIABLE_NO_TIMEOUTS, RELIABLE_NO_TIMEOUTS, actions, leaveInstance,
                                          ref -> ref.get().get().syncThrowUncheckedOnInterrupt(), preparedUnbootstrap),
            // setup the node's own gossip state for natural ownership, and return gossip actions to disseminate
            new OnClusterUpdateGossip(actions, leaving, new OnInstanceSetLeft(actions, leaving))
        );
    }
}
