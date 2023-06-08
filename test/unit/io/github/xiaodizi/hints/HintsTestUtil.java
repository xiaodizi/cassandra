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
package io.github.xiaodizi.hints;

import java.util.UUID;

import com.google.common.collect.Iterators;

import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.partitions.AbstractBTreePartition;
import io.github.xiaodizi.db.partitions.PartitionUpdate;
import io.github.xiaodizi.gms.IFailureDetectionEventListener;
import io.github.xiaodizi.gms.IFailureDetector;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.net.Message;
import io.github.xiaodizi.net.MockMessagingService;
import io.github.xiaodizi.net.MockMessagingSpy;
import io.github.xiaodizi.net.NoPayload;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.service.StorageService;
import io.github.xiaodizi.utils.Clock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static io.github.xiaodizi.Util.dk;
import static io.github.xiaodizi.net.MockMessagingService.verb;
import static io.github.xiaodizi.net.Verb.HINT_REQ;
import static io.github.xiaodizi.net.Verb.HINT_RSP;

final class HintsTestUtil
{
    static void assertPartitionsEqual(AbstractBTreePartition expected, AbstractBTreePartition actual)
    {
        assertEquals(expected.partitionKey(), actual.partitionKey());
        assertEquals(expected.deletionInfo(), actual.deletionInfo());
        assertEquals(expected.columns(), actual.columns());
        assertTrue(Iterators.elementsEqual(expected.iterator(), actual.iterator()));
    }

    static void assertHintsEqual(Hint expected, Hint actual)
    {
        assertEquals(expected.mutation.getKeyspaceName(), actual.mutation.getKeyspaceName());
        assertEquals(expected.mutation.key(), actual.mutation.key());
        assertEquals(expected.mutation.getTableIds(), actual.mutation.getTableIds());
        for (PartitionUpdate partitionUpdate : expected.mutation.getPartitionUpdates())
            assertPartitionsEqual(partitionUpdate, actual.mutation.getPartitionUpdate(partitionUpdate.metadata()));
        assertEquals(expected.creationTime, actual.creationTime);
        assertEquals(expected.gcgs, actual.gcgs);
    }

    static MockMessagingSpy sendHintsAndResponses(TableMetadata metadata, int noOfHints, int noOfResponses)
    {
        // create spy for hint messages, but only create responses for noOfResponses hints
        Message<NoPayload> message = Message.internalResponse(HINT_RSP, NoPayload.noPayload);

        MockMessagingSpy spy;
        if (noOfResponses != -1)
        {
            spy = MockMessagingService.when(verb(HINT_REQ)).respondN(message, noOfResponses);
        }
        else
        {
            spy = MockMessagingService.when(verb(HINT_REQ)).respond(message);
        }

        // create and write noOfHints using service
        UUID hostId = StorageService.instance.getLocalHostUUID();
        for (int i = 0; i < noOfHints; i++)
        {
            long now = Clock.Global.currentTimeMillis();
            DecoratedKey dkey = dk(String.valueOf(i));
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, dkey).timestamp(now);
            builder.row("column0").add("val", "value0");
            Hint hint = Hint.create(builder.buildAsMutation(), now);
            HintsService.instance.write(hostId, hint);
        }
        return spy;
    }

    static class MockFailureDetector implements IFailureDetector
    {
        boolean isAlive = true;

        public boolean isAlive(InetAddressAndPort ep)
        {
            return isAlive;
        }

        public void interpret(InetAddressAndPort ep)
        {
            throw new UnsupportedOperationException();
        }

        public void report(InetAddressAndPort ep)
        {
            throw new UnsupportedOperationException();
        }

        public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            throw new UnsupportedOperationException();
        }

        public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            throw new UnsupportedOperationException();
        }

        public void remove(InetAddressAndPort ep)
        {
            throw new UnsupportedOperationException();
        }

        public void forceConviction(InetAddressAndPort ep)
        {
            throw new UnsupportedOperationException();
        }
    }
}
