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

package io.github.xiaodizi.service.reads.range;

import io.github.xiaodizi.db.partitions.PartitionIterator;
import io.github.xiaodizi.db.rows.RowIterator;
import io.github.xiaodizi.exceptions.ReadTimeoutException;
import io.github.xiaodizi.locator.EndpointsForRange;
import io.github.xiaodizi.locator.ReplicaPlan;
import io.github.xiaodizi.service.reads.DataResolver;
import io.github.xiaodizi.service.reads.ReadCallback;
import io.github.xiaodizi.service.reads.repair.ReadRepair;
import io.github.xiaodizi.utils.AbstractIterator;

class SingleRangeResponse extends AbstractIterator<RowIterator> implements PartitionIterator
{
    private final DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver;
    private final ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler;
    private final ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> readRepair;

    private PartitionIterator result;

    SingleRangeResponse(DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver,
                        ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler,
                        ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> readRepair)
    {
        this.resolver = resolver;
        this.handler = handler;
        this.readRepair = readRepair;
    }

    ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> getReadRepair()
    {
        return readRepair;
    }

    private void waitForResponse() throws ReadTimeoutException
    {
        if (result != null)
            return;

        handler.awaitResults();
        result = resolver.resolve();
    }

    @Override
    protected RowIterator computeNext()
    {
        waitForResponse();
        return result.hasNext() ? result.next() : endOfData();
    }

    @Override
    public void close()
    {
        if (result != null)
            result.close();
    }
}
