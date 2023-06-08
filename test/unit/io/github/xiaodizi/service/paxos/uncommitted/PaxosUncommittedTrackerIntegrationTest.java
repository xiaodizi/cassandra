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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.*;

import io.github.xiaodizi.SchemaLoader;
import io.github.xiaodizi.Util;
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.cql3.statements.schema.CreateTableStatement;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.schema.KeyspaceParams;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.service.paxos.Ballot;
import io.github.xiaodizi.service.paxos.PaxosState;
import io.github.xiaodizi.utils.ByteBufferUtil;
import io.github.xiaodizi.utils.CloseableIterator;

import static io.github.xiaodizi.service.paxos.Ballot.Flag.NONE;
import static io.github.xiaodizi.service.paxos.BallotGenerator.Global.nextBallot;
import static io.github.xiaodizi.service.paxos.Commit.*;
import static io.github.xiaodizi.service.paxos.uncommitted.PaxosUncommittedTests.ALL_RANGES;
import static io.github.xiaodizi.service.paxos.uncommitted.PaxosUncommittedTests.PAXOS_CFS;

public class PaxosUncommittedTrackerIntegrationTest
{
    protected static String ks;
    protected static final String tbl = "tbl";
    protected static TableMetadata cfm;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();

        ks = "coordinatorsessiontest";
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", ks).build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm);
    }

    @Before
    public void setUp() throws Exception
    {
        PAXOS_CFS.truncateBlocking();
    }

    private static DecoratedKey dk(int v)
    {
        return DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(v));
    }

    @Test
    public void commitCycle()
    {
        PaxosUncommittedTracker tracker = PaxosState.uncommittedTracker();
        PaxosBallotTracker ballotTracker = PaxosState.ballotTracker();
        Assert.assertNull(tracker.getTableState(cfm.id));
        Assert.assertEquals(Ballot.none(), ballotTracker.getLowBound());
        Assert.assertEquals(Ballot.none(), ballotTracker.getHighBound());

        try (CloseableIterator<UncommittedPaxosKey> iterator = tracker.uncommittedKeyIterator(cfm.id, ALL_RANGES))
        {
            Assert.assertFalse(iterator.hasNext());
        }

        DecoratedKey key = dk(1);
        Ballot ballot = nextBallot(NONE);
        Proposal proposal = new Proposal(ballot, PaxosRowsTest.nonEmptyUpdate(ballot, cfm, key));

        try (PaxosState state = PaxosState.get(key, cfm))
        {
            state.promiseIfNewer(proposal.ballot, true);
        }

        try (CloseableIterator<UncommittedPaxosKey> iterator = tracker.uncommittedKeyIterator(cfm.id, ALL_RANGES))
        {
            Assert.assertEquals(key, Iterators.getOnlyElement(iterator).getKey());
        }

        try (PaxosState state = PaxosState.get(key, cfm))
        {
            state.acceptIfLatest(proposal);
        }

        try (CloseableIterator<UncommittedPaxosKey> iterator = tracker.uncommittedKeyIterator(cfm.id, ALL_RANGES))
        {
            Assert.assertEquals(key, Iterators.getOnlyElement(iterator).getKey());
        }

        PaxosState.commitDirect(proposal.agreed());
        try (CloseableIterator<UncommittedPaxosKey> iterator = tracker.uncommittedKeyIterator(cfm.id, ALL_RANGES))
        {
            Assert.assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void inMemoryCommit()
    {
        PaxosUncommittedTracker tracker = PaxosState.uncommittedTracker();

        DecoratedKey key = dk(1);
        Ballot ballot = nextBallot(NONE);
        Proposal proposal = new Proposal(ballot, PaxosRowsTest.nonEmptyUpdate(ballot, cfm, key));

        try (PaxosState state = PaxosState.get(key, cfm))
        {
            state.promiseIfNewer(proposal.ballot, true);
            state.acceptIfLatest(proposal);
        }
        try (CloseableIterator<UncommittedPaxosKey> iterator = tracker.uncommittedKeyIterator(cfm.id, ALL_RANGES))
        {
            Assert.assertEquals(key, Iterators.getOnlyElement(iterator).getKey());
        }

        Util.flush(PAXOS_CFS);

        PaxosState.commitDirect(proposal.agreed());
        try (CloseableIterator<UncommittedPaxosKey> iterator = tracker.uncommittedKeyIterator(cfm.id, ALL_RANGES))
        {
            Assert.assertEquals(Lists.newArrayList(), Lists.newArrayList(iterator));
        }
    }
}
