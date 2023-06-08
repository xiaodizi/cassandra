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

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.repair.messages.RepairMessage;
import io.github.xiaodizi.repair.messages.SyncRequest;
import io.github.xiaodizi.streaming.PreviewKind;

import static io.github.xiaodizi.utils.TimeUUID.Generator.nextTimeUUID;

public class SymmetricRemoteSyncTaskTest extends AbstractRepairTest
{
    private static final RepairJobDesc DESC = new RepairJobDesc(nextTimeUUID(), nextTimeUUID(), "ks", "tbl", ALL_RANGES);
    private static final List<Range<Token>> RANGE_LIST = ImmutableList.of(RANGE1);
    private static class InstrumentedSymmetricRemoteSyncTask extends SymmetricRemoteSyncTask
    {
        public InstrumentedSymmetricRemoteSyncTask(InetAddressAndPort e1, InetAddressAndPort e2)
        {
            super(DESC, e1, e2, RANGE_LIST, PreviewKind.NONE);
        }

        RepairMessage sentMessage = null;
        InetAddressAndPort sentTo = null;

        @Override
        void sendRequest(SyncRequest request, InetAddressAndPort to)
        {
            Assert.assertNull(sentMessage);
            Assert.assertNotNull(request);
            Assert.assertNotNull(to);
            sentMessage = request;
            sentTo = to;
        }
    }

    @Test
    public void normalSync()
    {
        InstrumentedSymmetricRemoteSyncTask syncTask = new InstrumentedSymmetricRemoteSyncTask(PARTICIPANT1, PARTICIPANT2);
        syncTask.startSync();

        Assert.assertNotNull(syncTask.sentMessage);
        Assert.assertSame(SyncRequest.class, syncTask.sentMessage.getClass());
        Assert.assertEquals(PARTICIPANT1, syncTask.sentTo);
    }
}
