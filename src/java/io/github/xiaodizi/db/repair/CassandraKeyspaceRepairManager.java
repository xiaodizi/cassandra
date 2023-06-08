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

package io.github.xiaodizi.db.repair;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;

import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.locator.RangesAtEndpoint;
import io.github.xiaodizi.repair.KeyspaceRepairManager;
import io.github.xiaodizi.utils.TimeUUID;
import io.github.xiaodizi.utils.concurrent.Future;

public class CassandraKeyspaceRepairManager implements KeyspaceRepairManager
{
    private final Keyspace keyspace;

    public CassandraKeyspaceRepairManager(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    @Override
    public Future<List<Void>> prepareIncrementalRepair(TimeUUID sessionID,
                                                       Collection<ColumnFamilyStore> tables,
                                                       RangesAtEndpoint tokenRanges,
                                                       ExecutorService executor,
                                                       BooleanSupplier isCancelled)
    {
        PendingAntiCompaction pac = new PendingAntiCompaction(sessionID, tables, tokenRanges, executor, isCancelled);
        return pac.run();
    }
}
