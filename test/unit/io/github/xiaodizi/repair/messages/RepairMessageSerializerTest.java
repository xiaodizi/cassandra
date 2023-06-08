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

package io.github.xiaodizi.repair.messages;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import io.github.xiaodizi.io.IVersionedSerializer;
import io.github.xiaodizi.io.util.DataInputBuffer;
import io.github.xiaodizi.io.util.DataOutputBuffer;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.net.MessagingService;

import static com.google.common.collect.Sets.newHashSet;
import static io.github.xiaodizi.locator.InetAddressAndPort.getByName;
import static io.github.xiaodizi.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * verifies repair message serializers are working as advertised
 */
public class RepairMessageSerializerTest
{
    private static int MS_VERSION = MessagingService.current_version;

    private static <T extends RepairMessage> T serdes(IVersionedSerializer<T> serializer, T message)
    {
        int expectedSize = (int) serializer.serializedSize(message, MS_VERSION);
        try (DataOutputBuffer out = new DataOutputBuffer(expectedSize))
        {
            serializer.serialize(message, out, MS_VERSION);
            Assert.assertEquals(expectedSize, out.buffer().limit());
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return serializer.deserialize(in, MS_VERSION);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void prepareConsistentRequest() throws Exception
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.0.1");
        InetAddressAndPort peer1 = InetAddressAndPort.getByName("10.0.0.2");
        InetAddressAndPort peer2 = InetAddressAndPort.getByName("10.0.0.3");
        InetAddressAndPort peer3 = InetAddressAndPort.getByName("10.0.0.4");
        PrepareConsistentRequest expected =
            new PrepareConsistentRequest(nextTimeUUID(), coordinator, newHashSet(peer1, peer2, peer3));
        PrepareConsistentRequest actual = serdes(PrepareConsistentRequest.serializer, expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void prepareConsistentResponse() throws Exception
    {
        PrepareConsistentResponse expected =
            new PrepareConsistentResponse(nextTimeUUID(), getByName("10.0.0.2"), true);
        PrepareConsistentResponse actual = serdes(PrepareConsistentResponse.serializer, expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void failSession() throws Exception
    {
        FailSession expected = new FailSession(nextTimeUUID());
        FailSession actual = serdes(FailSession.serializer, expected);
        Assert.assertEquals(expected, actual);;
    }

    @Test
    public void finalizeCommit() throws Exception
    {
        FinalizeCommit expected = new FinalizeCommit(nextTimeUUID());
        FinalizeCommit actual = serdes(FinalizeCommit.serializer, expected);
        Assert.assertEquals(expected, actual);;
    }

    @Test
    public void finalizePromise() throws Exception
    {
        FinalizePromise expected = new FinalizePromise(nextTimeUUID(), getByName("10.0.0.2"), true);
        FinalizePromise actual = serdes(FinalizePromise.serializer, expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void finalizePropose() throws Exception
    {
        FinalizePropose expected = new FinalizePropose(nextTimeUUID());
        FinalizePropose actual = serdes(FinalizePropose.serializer, expected);
        Assert.assertEquals(expected, actual);;
    }
}
