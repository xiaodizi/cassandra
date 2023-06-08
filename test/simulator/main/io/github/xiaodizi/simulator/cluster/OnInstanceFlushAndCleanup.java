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

import io.github.xiaodizi.Util;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.compaction.CompactionManager;
import io.github.xiaodizi.distributed.api.IIsolatedExecutor;
import io.github.xiaodizi.simulator.Action;

import java.util.function.Function;

class OnInstanceFlushAndCleanup extends ClusterReliableAction
{
    OnInstanceFlushAndCleanup(ClusterActions actions, int on)
    {
        super("Flush and Cleanup on " + on, actions, on, invokableFlushAndCleanup());
    }

    public static Function<Integer, Action> factory(ClusterActions actions)
    {
        return (on) -> new OnInstanceFlushAndCleanup(actions, on);
    }

    private static IIsolatedExecutor.SerializableRunnable invokableFlushAndCleanup()
    {
        return () -> {
            for (Keyspace keyspace : Keyspace.all())
            {
                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                {
                    try
                    {
                        Util.flush(cfs);
                        if (cfs.forceCleanup(1) != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                            throw new IllegalStateException();
                        cfs.forceMajorCompaction();
                    }
                    catch (Throwable t) { throw new RuntimeException(t); }
                }
            }
        };
    }

}
