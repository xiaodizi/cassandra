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

import java.io.IOException;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import io.github.xiaodizi.SchemaLoader;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.schema.Schema;
import io.github.xiaodizi.db.Mutation;
import io.github.xiaodizi.db.RowUpdateBuilder;
import io.github.xiaodizi.io.util.DataInputBuffer;
import io.github.xiaodizi.io.util.DataInputPlus;
import io.github.xiaodizi.io.util.DataOutputBuffer;
import io.github.xiaodizi.net.MessagingService;
import io.github.xiaodizi.schema.KeyspaceParams;
import io.github.xiaodizi.utils.FBUtilities;

import static io.github.xiaodizi.hints.HintsTestUtil.assertHintsEqual;
import static io.github.xiaodizi.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HintMessageTest
{
    private static final String KEYSPACE = "hint_message_test";
    private static final String TABLE = "table";

    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Test
    public void testSerializer() throws IOException
    {
        UUID hostId = UUID.randomUUID();
        long now = FBUtilities.timestampMicros();
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        
        Mutation mutation = 
            new RowUpdateBuilder(table, now, bytes("key")).clustering("column").add("val", "val" + 1234).build();
        
        Hint hint = Hint.create(mutation, now / 1000);
        HintMessage message = new HintMessage(hostId, hint);

        // serialize
        int serializedSize = (int) HintMessage.serializer.serializedSize(message, MessagingService.current_version);
        HintMessage deserializedMessage;
        
        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            HintMessage.serializer.serialize(message, dob, MessagingService.current_version);
            assertEquals(serializedSize, dob.getLength());

            // deserialize
            DataInputPlus di = new DataInputBuffer(dob.buffer(), true);
            deserializedMessage = HintMessage.serializer.deserialize(di, MessagingService.current_version);
        }

        // compare before/after
        assertEquals(hostId, deserializedMessage.hostId);
        assertNotNull(deserializedMessage.hint);
        assertHintsEqual(hint, deserializedMessage.hint);
    }

    @Test
    public void testEncodedSerializer() throws IOException
    {
        UUID hostId = UUID.randomUUID();
        long now = FBUtilities.timestampMicros();
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        
        Mutation mutation =
            new RowUpdateBuilder(table, now, bytes("key")).clustering("column").add("val", "val" + 1234) .build();
        
        Hint hint = Hint.create(mutation, now / 1000);
        HintMessage.Encoded message;
        
        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            Hint.serializer.serialize(hint, dob, MessagingService.current_version);
            message = new HintMessage.Encoded(hostId, dob.buffer(), MessagingService.current_version);
        } 

        // serialize
        int serializedSize = (int) HintMessage.serializer.serializedSize(message, MessagingService.current_version);
        DataOutputBuffer dob = new DataOutputBuffer();
        HintMessage.serializer.serialize(message, dob, MessagingService.current_version);
        assertEquals(serializedSize, dob.getLength());

        // deserialize
        DataInputPlus dip = new DataInputBuffer(dob.buffer(), true);
        HintMessage deserializedMessage = HintMessage.serializer.deserialize(dip, MessagingService.current_version);

        // compare before/after
        assertEquals(hostId, deserializedMessage.hostId);
        assertNotNull(deserializedMessage.hint);
        assertHintsEqual(hint, deserializedMessage.hint);
    }
}
