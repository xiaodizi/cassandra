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
package io.github.xiaodizi.repair;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import io.github.xiaodizi.concurrent.ExecutorPlus;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.repair.consistent.CoordinatorSession;
import io.github.xiaodizi.repair.messages.RepairOption;
import io.github.xiaodizi.service.ActiveRepairService;
import io.github.xiaodizi.utils.FBUtilities;
import io.github.xiaodizi.utils.TimeUUID;
import io.github.xiaodizi.utils.concurrent.Future;

public class IncrementalRepairTask extends AbstractRepairTask
{
    private final TimeUUID parentSession;
    private final RepairRunnable.NeighborsAndRanges neighborsAndRanges;
    private final String[] cfnames;

    protected IncrementalRepairTask(RepairOption options,
                                    String keyspace,
                                    RepairNotifier notifier,
                                    TimeUUID parentSession,
                                    RepairRunnable.NeighborsAndRanges neighborsAndRanges,
                                    String[] cfnames)
    {
        super(options, keyspace, notifier);
        this.parentSession = parentSession;
        this.neighborsAndRanges = neighborsAndRanges;
        this.cfnames = cfnames;
    }

    @Override
    public String name()
    {
        return "Repair";
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor) throws Exception
    {
        // the local node also needs to be included in the set of participants, since coordinator sessions aren't persisted
        Set<InetAddressAndPort> allParticipants = ImmutableSet.<InetAddressAndPort>builder()
                                                  .addAll(neighborsAndRanges.participants)
                                                  .add(FBUtilities.getBroadcastAddressAndPort())
                                                  .build();
        // Not necessary to include self for filtering. The common ranges only contains neighbhor node endpoints.
        List<CommonRange> allRanges = neighborsAndRanges.filterCommonRanges(keyspace, cfnames);

        CoordinatorSession coordinatorSession = ActiveRepairService.instance.consistent.coordinated.registerSession(parentSession, allParticipants, neighborsAndRanges.shouldExcludeDeadParticipants);

        return coordinatorSession.execute(() -> runRepair(parentSession, true, executor, allRanges, cfnames));

    }
}
