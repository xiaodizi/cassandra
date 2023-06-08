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
package io.github.xiaodizi.tools.nodetool;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import io.github.xiaodizi.tools.NodeProbe;
import io.github.xiaodizi.tools.NodeTool.NodeToolCmd;

@Command(name = "setlogginglevel", description = "Set the log level threshold for a given component or class. Will reset to the initial configuration if called with no parameters.")
public class SetLoggingLevel extends NodeToolCmd
{
    @Arguments(usage = "<component|class> <level>", description = "The component or class to change the level for and the log level threshold to set. Will reset to initial level if omitted. "
        + "Available components:  bootstrap, compaction, repair, streaming, cql, ring")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        String target = args.size() >= 1 ? args.get(0) : EMPTY;
        String level = args.size() == 2 ? args.get(1) : EMPTY;

        List<String> classQualifiers = Collections.singletonList(target);
        if (target.equals("bootstrap"))
        {
            classQualifiers = Lists.newArrayList(
                    "io.github.xiaodizi.gms",
                    "io.github.xiaodizi.hints",
                    "io.github.xiaodizi.schema",
                    "io.github.xiaodizi.service.StorageService",
                    "io.github.xiaodizi.db.SystemKeyspace",
                    "io.github.xiaodizi.batchlog.BatchlogManager",
                    "io.github.xiaodizi.net.MessagingService");
        }
        else if (target.equals("repair"))
        {
            classQualifiers = Lists.newArrayList(
                    "io.github.xiaodizi.repair",
                    "io.github.xiaodizi.db.compaction.CompactionManager",
                    "io.github.xiaodizi.service.SnapshotVerbHandler");
        }
        else if (target.equals("streaming"))
        {
            classQualifiers = Lists.newArrayList(
                    "io.github.xiaodizi.streaming",
                    "io.github.xiaodizi.dht.RangeStreamer");
        }
        else if (target.equals("compaction"))
        {
            classQualifiers = Lists.newArrayList(
                    "io.github.xiaodizi.db.compaction",
                    "io.github.xiaodizi.db.ColumnFamilyStore",
                    "io.github.xiaodizi.io.sstable.IndexSummaryRedistribution");
        }
        else if (target.equals("cql"))
        {
            classQualifiers = Lists.newArrayList(
                    "io.github.xiaodizi.cql3",
                    "io.github.xiaodizi.auth",
                    "io.github.xiaodizi.batchlog",
                    "io.github.xiaodizi.net.ResponseVerbHandler",
                    "io.github.xiaodizi.service.AbstractReadExecutor",
                    "io.github.xiaodizi.service.AbstractWriteResponseHandler",
                    "io.github.xiaodizi.service.paxos",
                    "io.github.xiaodizi.service.ReadCallback",
                    "io.github.xiaodizi.service.ResponseResolver");
        }
        else if (target.equals("ring"))
        {
            classQualifiers = Lists.newArrayList(
                    "io.github.xiaodizi.gms",
                    "io.github.xiaodizi.service.PendingRangeCalculatorService",
                    "io.github.xiaodizi.service.LoadBroadcaster",
                    "io.github.xiaodizi.transport.Server");
        }

        for (String classQualifier : classQualifiers)
            probe.setLoggingLevel(classQualifier, level);
    }
}
