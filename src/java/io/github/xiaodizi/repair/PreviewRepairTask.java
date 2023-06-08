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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import io.github.xiaodizi.concurrent.ExecutorPlus;
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.metrics.RepairMetrics;
import io.github.xiaodizi.repair.consistent.SyncStatSummary;
import io.github.xiaodizi.repair.messages.RepairOption;
import io.github.xiaodizi.streaming.PreviewKind;
import io.github.xiaodizi.utils.DiagnosticSnapshotService;
import io.github.xiaodizi.utils.TimeUUID;
import io.github.xiaodizi.utils.concurrent.Future;

public class PreviewRepairTask extends AbstractRepairTask
{
    private final TimeUUID parentSession;
    private final List<CommonRange> commonRanges;
    private final String[] cfnames;
    private volatile String successMessage = name() + " completed successfully";

    protected PreviewRepairTask(RepairOption options, String keyspace, RepairNotifier notifier, TimeUUID parentSession, List<CommonRange> commonRanges, String[] cfnames)
    {
        super(options, keyspace, notifier);
        this.parentSession = parentSession;
        this.commonRanges = commonRanges;
        this.cfnames = cfnames;
    }

    @Override
    public String name()
    {
        return "Repair preview";
    }

    @Override
    public String successMessage()
    {
        return successMessage;
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor)
    {
        Future<CoordinatedRepairResult> f = runRepair(parentSession, false, executor, commonRanges, cfnames);
        return f.map(result -> {
            if (result.hasFailed())
                return result;

            PreviewKind previewKind = options.getPreviewKind();
            Preconditions.checkState(previewKind != PreviewKind.NONE, "Preview is NONE");
            SyncStatSummary summary = new SyncStatSummary(true);
            summary.consumeSessionResults(result.results);

            final String message;
            if (summary.isEmpty())
            {
                message = previewKind == PreviewKind.REPAIRED ? "Repaired data is in sync" : "Previewed data was in sync";
            }
            else
            {
                message = (previewKind == PreviewKind.REPAIRED ? "Repaired data is inconsistent\n" : "Preview complete\n") + summary;
                RepairMetrics.previewFailures.inc();
                if (previewKind == PreviewKind.REPAIRED)
                    maybeSnapshotReplicas(parentSession, keyspace, result.results.get()); // we know its present as summary used it
            }
            successMessage += "; " + message;
            notifier.notification(message);

            return result;
        });
    }

    private void maybeSnapshotReplicas(TimeUUID parentSession, String keyspace, List<RepairSessionResult> results)
    {
        if (!DatabaseDescriptor.snapshotOnRepairedDataMismatch())
            return;

        try
        {
            Set<String> mismatchingTables = new HashSet<>();
            Set<InetAddressAndPort> nodes = new HashSet<>();
            for (RepairSessionResult sessionResult : results)
            {
                for (RepairResult repairResult : emptyIfNull(sessionResult.repairJobResults))
                {
                    for (SyncStat stat : emptyIfNull(repairResult.stats))
                    {
                        if (stat.numberOfDifferences > 0)
                            mismatchingTables.add(repairResult.desc.columnFamily);
                        // snapshot all replicas, even if they don't have any differences
                        nodes.add(stat.nodes.coordinator);
                        nodes.add(stat.nodes.peer);
                    }
                }
            }

            String snapshotName = DiagnosticSnapshotService.getSnapshotName(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX);
            for (String table : mismatchingTables)
            {
                // we can just check snapshot existence locally since the repair coordinator is always a replica (unlike in the read case)
                if (!Keyspace.open(keyspace).getColumnFamilyStore(table).snapshotExists(snapshotName))
                {
                    logger.info("{} Snapshotting {}.{} for preview repair mismatch with tag {} on instances {}",
                                options.getPreviewKind().logPrefix(parentSession),
                                keyspace, table, snapshotName, nodes);
                    DiagnosticSnapshotService.repairedDataMismatch(Keyspace.open(keyspace).getColumnFamilyStore(table).metadata(), nodes);
                }
                else
                {
                    logger.info("{} Not snapshotting {}.{} - snapshot {} exists",
                                options.getPreviewKind().logPrefix(parentSession),
                                keyspace, table, snapshotName);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("{} Failed snapshotting replicas", options.getPreviewKind().logPrefix(parentSession), e);
        }
    }

    private static <T> Iterable<T> emptyIfNull(Iterable<T> iter)
    {
        if (iter == null)
            return Collections.emptyList();
        return iter;
    }
}
