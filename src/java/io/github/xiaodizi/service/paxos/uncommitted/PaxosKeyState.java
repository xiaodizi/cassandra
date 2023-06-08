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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

import io.github.xiaodizi.db.ConsistencyLevel;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.schema.TableId;
import io.github.xiaodizi.service.paxos.Ballot;
import io.github.xiaodizi.utils.CloseableIterator;
import io.github.xiaodizi.utils.MergeIterator;

public class PaxosKeyState implements UncommittedPaxosKey
{
    static final Comparator<PaxosKeyState> KEY_COMPARATOR = Comparator.comparing(o -> o.key);
    static final Comparator<PaxosKeyState> BALLOT_COMPARATOR = (o1, o2) -> Longs.compare(o1.ballot.uuidTimestamp(), o2.ballot.uuidTimestamp());

    final TableId tableId;
    final DecoratedKey key;
    final Ballot ballot;
    final boolean committed;

    public PaxosKeyState(TableId tableId, DecoratedKey key, Ballot ballot, boolean committed)
    {
        Preconditions.checkNotNull(tableId);
        Preconditions.checkNotNull(ballot);
        this.tableId = tableId;
        this.key = key;
        this.ballot = ballot;
        this.committed = committed;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        switch (ballot.flag())
        {
            default: throw new IllegalStateException();
            case GLOBAL: return ConsistencyLevel.SERIAL;
            case LOCAL: return ConsistencyLevel.LOCAL_SERIAL;
            case NONE: return null;
        }
    }

    @Override
    public Ballot ballot()
    {
        return ballot;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaxosKeyState that = (PaxosKeyState) o;
        return committed == that.committed &&
               Objects.equals(key, that.key) &&
               Objects.equals(ballot, that.ballot);
    }

    public int hashCode()
    {
        return Objects.hash(key, ballot, committed);
    }

    public String toString()
    {
        return "BallotState{" +
               "tableId=" + tableId +
               ", key=" + key +
               ", ballot=" + ballot +
               ", committed=" + committed +
               '}';
    }

    static PaxosKeyState merge(PaxosKeyState left, PaxosKeyState right)
    {
        if (left == null)
            return right;

        if (right == null)
            return left;

        int cmp = BALLOT_COMPARATOR.compare(left, right);

        // prefer committed operations if the ballots are the same so they can be filtered out later
        if (cmp == 0)
            return left.committed ? left : right;
        else
            return cmp > 0 ? left : right;
    }

    static class Reducer extends MergeIterator.Reducer<PaxosKeyState, PaxosKeyState>
    {
        private PaxosKeyState mostRecent = null;

        public void reduce(int idx, PaxosKeyState current)
        {
            mostRecent = merge(mostRecent, current);
        }

        protected PaxosKeyState getReduced()
        {
            return mostRecent;
        }

        protected void onKeyChange()
        {
            super.onKeyChange();
            mostRecent = null;
        }
    }

    public static CloseableIterator<PaxosKeyState> mergeUncommitted(CloseableIterator<PaxosKeyState>... iterators)
    {
        return MergeIterator.get(Lists.newArrayList(iterators), PaxosKeyState.KEY_COMPARATOR, new Reducer());
    }

    public static CloseableIterator<UncommittedPaxosKey> toUncommittedInfo(CloseableIterator<PaxosKeyState> iter)
    {
        Iterator<PaxosKeyState> filtered = Iterators.filter(iter, k -> !k.committed);
        return new CloseableIterator<UncommittedPaxosKey>()
        {
            public void close()
            {
                iter.close();
            }

            public boolean hasNext()
            {
                return filtered.hasNext();
            }

            public UncommittedPaxosKey next()
            {
                return filtered.next();
            }
        };
    }
}
