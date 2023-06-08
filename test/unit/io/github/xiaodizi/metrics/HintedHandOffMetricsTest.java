/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package io.github.xiaodizi.metrics;

import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterators;

import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.cql3.UntypedResultSet;
import io.github.xiaodizi.db.SystemKeyspace;
import io.github.xiaodizi.db.commitlog.CommitLog;
import io.github.xiaodizi.db.marshal.Int32Type;
import io.github.xiaodizi.db.marshal.UUIDType;
import io.github.xiaodizi.hints.HintsService;
import io.github.xiaodizi.locator.InetAddressAndPort;

import static org.junit.Assert.assertEquals;
import static io.github.xiaodizi.cql3.QueryProcessor.executeInternal;

public class HintedHandOffMetricsTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Test
    public void testHintsMetrics() throws Exception
    {
        DatabaseDescriptor.getHintsDirectory().tryCreateDirectories();

        for (int i = 0; i < 99; i++)
            HintsService.instance.metrics.incrPastWindow(InetAddressAndPort.getLocalHost());
        HintsService.instance.metrics.log();

        UntypedResultSet rows = executeInternal("SELECT hints_dropped FROM system." + SystemKeyspace.PEER_EVENTS_V2);
        Map<UUID, Integer> returned = rows.one().getMap("hints_dropped", UUIDType.instance, Int32Type.instance);
        assertEquals(Iterators.getLast(returned.values().iterator()).intValue(), 99);

        rows = executeInternal("SELECT hints_dropped FROM system." + SystemKeyspace.LEGACY_PEER_EVENTS);
        returned = rows.one().getMap("hints_dropped", UUIDType.instance, Int32Type.instance);
        assertEquals(Iterators.getLast(returned.values().iterator()).intValue(), 99);
    }
}
