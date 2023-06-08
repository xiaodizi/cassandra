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

package io.github.xiaodizi.service.paxos.uncommitted;

import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.PartitionPosition;
import io.github.xiaodizi.db.memtable.Memtable;
import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.schema.TableId;
import io.github.xiaodizi.service.paxos.Ballot;
import io.github.xiaodizi.utils.CloseableIterator;

public class PaxosMockUpdateSupplier implements PaxosUncommittedTracker.UpdateSupplier
{
    private final Map<TableId, NavigableMap<PartitionPosition, PaxosKeyState>> states = new HashMap<>();

    private NavigableMap<PartitionPosition, PaxosKeyState> mapFor(TableId tableId)
    {
        return states.computeIfAbsent(tableId, key -> new TreeMap<>());
    }

    private void updateTo(TableId tableId, PaxosKeyState newState)
    {
        NavigableMap<PartitionPosition, PaxosKeyState> map = mapFor(tableId);
        PaxosKeyState current = map.get(newState.key);
        if (current != null && PaxosKeyState.BALLOT_COMPARATOR.compare(current, newState) > 0)
            return;

        map.put(newState.key, newState);
    }

    void inProgress(TableId tableId, DecoratedKey key, Ballot ballot)
    {
        updateTo(tableId, new PaxosKeyState(tableId, key, ballot, false));
    }

    void committed(TableId tableId, DecoratedKey key, Ballot ballot)
    {
        updateTo(tableId, new PaxosKeyState(tableId, key, ballot, true));
    }

    public CloseableIterator<PaxosKeyState> repairIterator(TableId tableId, Collection<Range<Token>> ranges)
    {
        Iterator<PaxosKeyState> iterator = Iterators.filter(mapFor(tableId).values().iterator(), k -> Iterables.any(ranges, r -> r.contains(k.key.getToken())));

        return new CloseableIterator<PaxosKeyState>()
        {
            public void close() {}

            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            public PaxosKeyState next()
            {
                return iterator.next();
            }
        };
    }

    public CloseableIterator<PaxosKeyState> flushIterator(Memtable memtable)
    {
        ArrayList<PaxosKeyState> keyStates = new ArrayList<>();
        for (Map.Entry<TableId, NavigableMap<PartitionPosition, PaxosKeyState>> statesEntry : states.entrySet())
        {
            for (Map.Entry<PartitionPosition, PaxosKeyState> entry : statesEntry.getValue().entrySet())
            {
                keyStates.add(entry.getValue());
            }
        }
        states.clear();

        Iterator<PaxosKeyState> iterator = keyStates.iterator();

        return new CloseableIterator<PaxosKeyState>()
        {
            public void close() {}

            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            public PaxosKeyState next()
            {
                return iterator.next();
            }
        };
    }
}
