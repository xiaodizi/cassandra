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
package io.github.xiaodizi.service;

import java.net.InetAddress;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.xiaodizi.utils.concurrent.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xiaodizi.db.TruncateResponse;
import io.github.xiaodizi.exceptions.RequestFailureReason;
import io.github.xiaodizi.exceptions.TruncateException;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.net.RequestCallback;
import io.github.xiaodizi.net.Message;
import io.github.xiaodizi.utils.concurrent.UncheckedInterruptedException;


import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static io.github.xiaodizi.config.DatabaseDescriptor.getTruncateRpcTimeout;
import static io.github.xiaodizi.utils.Clock.Global.nanoTime;
import static io.github.xiaodizi.utils.concurrent.Condition.newOneTimeCondition;

public class TruncateResponseHandler implements RequestCallback<TruncateResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger(TruncateResponseHandler.class);
    protected final Condition condition = newOneTimeCondition();
    private final int responseCount;
    protected final AtomicInteger responses = new AtomicInteger(0);
    private final long start;
    private volatile InetAddress truncateFailingReplica;

    public TruncateResponseHandler(int responseCount)
    {
        // at most one node per range can bootstrap at a time, and these will be added to the write until
        // bootstrap finishes (at which point we no longer need to write to the old ones).
        assert 1 <= responseCount: "invalid response count " + responseCount;

        this.responseCount = responseCount;
        start = nanoTime();
    }

    public void get() throws TimeoutException
    {
        long timeoutNanos = getTruncateRpcTimeout(NANOSECONDS) - (nanoTime() - start);
        boolean completedInTime;
        try
        {
            completedInTime = condition.await(timeoutNanos, NANOSECONDS); // TODO truncate needs a much longer timeout
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        if (!completedInTime)
        {
            throw new TimeoutException("Truncate timed out - received only " + responses.get() + " responses");
        }

        if (truncateFailingReplica != null)
        {
            throw new TruncateException("Truncate failed on replica " + truncateFailingReplica);
        }
    }

    @Override
    public void onResponse(Message<TruncateResponse> message)
    {
        responses.incrementAndGet();
        if (responses.get() >= responseCount)
            condition.signalAll();
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        // If the truncation hasn't succeeded on some replica, abort and indicate this back to the client.
        truncateFailingReplica = from.getAddress();
        condition.signalAll();
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }
}
