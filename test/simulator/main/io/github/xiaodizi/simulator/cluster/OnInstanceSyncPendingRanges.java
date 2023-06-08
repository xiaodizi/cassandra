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

import io.github.xiaodizi.service.PendingRangeCalculatorService;
import io.github.xiaodizi.simulator.Action;
import io.github.xiaodizi.simulator.systems.SimulatedActionTask;

import java.util.function.Function;

import static io.github.xiaodizi.simulator.Action.Modifiers.RELIABLE;

class OnInstanceSyncPendingRanges extends SimulatedActionTask
{
    OnInstanceSyncPendingRanges(ClusterActions actions, int node)
    {
        //noinspection Convert2MethodRef - invalid inspection across multiple classloaders
        super("Sync Pending Ranges on " + node, RELIABLE, RELIABLE, actions, actions.cluster.get(node),
              () -> PendingRangeCalculatorService.instance.blockUntilFinished());
    }

    public static Function<Integer, Action> factory(ClusterActions clusterActions)
    {
        return (node) -> new OnInstanceSyncPendingRanges(clusterActions, node);
    }
}
