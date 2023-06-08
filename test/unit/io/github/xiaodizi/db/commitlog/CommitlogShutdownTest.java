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

package io.github.xiaodizi.db.commitlog;

import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import io.github.xiaodizi.io.util.File;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.github.xiaodizi.SchemaLoader;
import io.github.xiaodizi.config.Config;
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.config.ParameterizedClass;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.Mutation;
import io.github.xiaodizi.db.RowUpdateBuilder;
import io.github.xiaodizi.db.compaction.CompactionManager;
import io.github.xiaodizi.db.marshal.AsciiType;
import io.github.xiaodizi.db.marshal.BytesType;
import io.github.xiaodizi.schema.KeyspaceParams;
import io.github.xiaodizi.schema.TableId;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

/**
 * Since this test depends on byteman rules being setup during initialization, you shouldn't add tests to this class
 */
@RunWith(BMUnitRunner.class)
public class CommitlogShutdownTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String STANDARD1 = "Standard1";

    private final static byte[] entropy = new byte[1024 * 256];

    @Test
    @BMRule(name = "Make removing commitlog segments slow",
    targetClass = "CommitLogSegment",
    targetMethod = "discard",
    action = "Thread.sleep(50)")
    public void testShutdownWithPendingTasks() throws Exception
    {
        new Random().nextBytes(entropy);
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of()));
        DatabaseDescriptor.setCommitLogSegmentSize(1);
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(10 * 1000);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));

                                    CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        final Mutation m = new RowUpdateBuilder(cfs1.metadata.get(), 0, "k")
                           .clustering("bytes")
                           .add("val", ByteBuffer.wrap(entropy))
                           .build();

        // force creating several commitlog files
        for (int i = 0; i < 10; i++)
        {
            CommitLog.instance.add(m);
        }

        // schedule discarding completed segments and immediately issue a shutdown
        TableId tableId = m.getTableIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(tableId, CommitLogPosition.NONE, CommitLog.instance.getCurrentPosition());
        CommitLog.instance.shutdownBlocking();

        // the shutdown should block until all logs except the currently active one and perhaps a new, empty one are gone
        Assert.assertTrue(new File(DatabaseDescriptor.getCommitLogLocation()).tryList().length <= 2);
    }
}
