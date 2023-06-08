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
package io.github.xiaodizi.streaming.messages;

import java.io.IOException;

import io.github.xiaodizi.db.TypeSizes;
import io.github.xiaodizi.io.util.DataInputPlus;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.streaming.StreamingChannel;
import io.github.xiaodizi.streaming.StreamingDataOutputPlus;
import io.github.xiaodizi.streaming.StreamOperation;
import io.github.xiaodizi.streaming.PreviewKind;
import io.github.xiaodizi.streaming.StreamResultFuture;
import io.github.xiaodizi.streaming.StreamSession;
import io.github.xiaodizi.utils.TimeUUID;

import static io.github.xiaodizi.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

/**
 * StreamInitMessage is first sent from the node where {@link io.github.xiaodizi.streaming.StreamSession} is started,
 * to initiate corresponding {@link io.github.xiaodizi.streaming.StreamSession} on the other side.
 */
public class StreamInitMessage extends StreamMessage
{
    public static Serializer<StreamInitMessage> serializer = new StreamInitMessageSerializer();

    public final InetAddressAndPort from;
    public final int sessionIndex;
    public final TimeUUID planId;
    public final StreamOperation streamOperation;

    public final TimeUUID pendingRepair;
    public final PreviewKind previewKind;

    public StreamInitMessage(InetAddressAndPort from, int sessionIndex, TimeUUID planId, StreamOperation streamOperation,
                             TimeUUID pendingRepair, PreviewKind previewKind)
    {
        super(Type.STREAM_INIT);
        this.from = from;
        this.sessionIndex = sessionIndex;
        this.planId = planId;
        this.streamOperation = streamOperation;
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;
    }

    @Override
    public StreamSession getOrCreateAndAttachInboundSession(StreamingChannel channel, int messagingVersion)
    {
        StreamSession session = StreamResultFuture.createFollower(sessionIndex, planId, streamOperation, from, channel, messagingVersion, pendingRepair, previewKind)
                                 .getSession(from, sessionIndex);
        session.attachInbound(channel);
        return session;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(128);
        sb.append("StreamInitMessage: from = ").append(from);
        sb.append(", planId = ").append(planId).append(", session index = ").append(sessionIndex);
        return sb.toString();
    }

    private static class StreamInitMessageSerializer implements Serializer<StreamInitMessage>
    {
        public void serialize(StreamInitMessage message, StreamingDataOutputPlus out, int version, StreamSession session) throws IOException
        {
            inetAddressAndPortSerializer.serialize(message.from, out, version);
            out.writeInt(message.sessionIndex);
            message.planId.serialize(out);
            out.writeUTF(message.streamOperation.getDescription());

            out.writeBoolean(message.pendingRepair != null);
            if (message.pendingRepair != null)
                message.pendingRepair.serialize(out);
            out.writeInt(message.previewKind.getSerializationVal());
        }

        public StreamInitMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            InetAddressAndPort from = inetAddressAndPortSerializer.deserialize(in, version);
            int sessionIndex = in.readInt();
            TimeUUID planId = TimeUUID.deserialize(in);
            String description = in.readUTF();

            TimeUUID pendingRepair = in.readBoolean() ? TimeUUID.deserialize(in) : null;
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new StreamInitMessage(from, sessionIndex, planId, StreamOperation.fromString(description),
                                         pendingRepair, previewKind);
        }

        public long serializedSize(StreamInitMessage message, int version)
        {
            long size = inetAddressAndPortSerializer.serializedSize(message.from, version);
            size += TypeSizes.sizeof(message.sessionIndex);
            size += TimeUUID.sizeInBytes();
            size += TypeSizes.sizeof(message.streamOperation.getDescription());
            size += TypeSizes.sizeof(message.pendingRepair != null);
            if (message.pendingRepair != null)
                size += TimeUUID.sizeInBytes();
            size += TypeSizes.sizeof(message.previewKind.getSerializationVal());

            return size;
        }
    }
}
