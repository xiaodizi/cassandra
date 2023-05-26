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

package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;

import org.junit.Test;

public class StreamFailureLogsFailureDueToSessionFailedTest extends AbstractStreamFailureLogs
{
<<<<<<<< HEAD:test/distributed/org/apache/cassandra/distributed/test/streaming/StreamFailureLogsFailureDueToSessionFailedTest.java
    @Test
    public void failureDueToSessionFailed() throws IOException
    {
        streamTest(true,"Remote peer /127.0.0.2:7012 failed stream session", 1);
========
    IndexSummary getIndexSummary();

    T cloneWithNewSummarySamplingLevel(ColumnFamilyStore cfs, int newSamplingLevel) throws IOException;

    static boolean isSupportedBy(SSTableFormat<?, ?> format)
    {
        return IndexSummarySupport.class.isAssignableFrom(format.getReaderFactory().getReaderClass());
>>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f:src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummarySupport.java
    }
}
