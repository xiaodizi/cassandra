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

import java.io.IOException;

import io.github.xiaodizi.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.db.Mutation;
import io.github.xiaodizi.db.rows.DeserializationHelper;
import io.github.xiaodizi.io.util.DataInputBuffer;
import io.github.xiaodizi.io.util.RebufferingInputStream;

/**
 * Utility class that flags the replayer as having seen a CDC mutation and calculates offset but doesn't apply mutations
 */
public class CDCTestReplayer extends CommitLogReplayer
{
    private static final Logger logger = LoggerFactory.getLogger(CDCTestReplayer.class);

    public CDCTestReplayer() throws IOException
    {
        super(CommitLog.instance, CommitLogPosition.NONE, null, ReplayFilter.create());
        CommitLog.instance.sync(true);
        commitLogReader = new CommitLogTestReader();
    }

    public void examineCommitLog() throws IOException
    {
        replayFiles(new File(DatabaseDescriptor.getCommitLogLocation()).tryList());
    }

    private class CommitLogTestReader extends CommitLogReader
    {
        @Override
        public void readAllFiles(CommitLogReadHandler handler, File[] files) throws IOException {
            super.readAllFiles(handler, files);
        }

        @Override
        protected void readMutation(CommitLogReadHandler handler,
                                    byte[] inputBuffer,
                                    int size,
                                    CommitLogPosition minPosition,
                                    final int entryLocation,
                                    final CommitLogDescriptor desc) throws IOException
        {
            RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size);
            Mutation mutation;
            try
            {
                mutation = Mutation.serializer.deserialize(bufIn, desc.getMessagingVersion(), DeserializationHelper.Flag.LOCAL);
                if (mutation.trackedByCDC())
                    sawCDCMutation = true;
            }
            catch (IOException e)
            {
                // Test fails.
                throw new AssertionError(e);
            }
        }
    }
}
