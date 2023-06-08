/**
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

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xiaodizi.concurrent.SequentialExecutorPlus;
import io.github.xiaodizi.concurrent.SequentialExecutorPlus.AtLeastOnceTrigger;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.locator.AbstractReplicationStrategy;
import io.github.xiaodizi.schema.Schema;
import io.github.xiaodizi.utils.ExecutorUtils;

import static io.github.xiaodizi.concurrent.ExecutorFactory.Global.executorFactory;
import static io.github.xiaodizi.utils.Clock.Global.currentTimeMillis;

public class PendingRangeCalculatorService
{
    public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService();

    private static final Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);

    // the executor will only run a single range calculation at a time while keeping at most one task queued in order
    // to trigger an update only after the most recent state change and not for each update individually
    private final SequentialExecutorPlus executor = executorFactory()
            .withJmxInternal()
            .configureSequential("PendingRangeCalculator")
            .withRejectedExecutionHandler((r, e) -> {})  // silently handle rejected tasks, this::update takes care of bookkeeping
            .build();

    private final AtLeastOnceTrigger update = executor.atLeastOnceTrigger(() -> {
        PendingRangeCalculatorServiceDiagnostics.taskStarted(1);
        long start = currentTimeMillis();
        Collection<String> keyspaces = Schema.instance.getNonLocalStrategyKeyspaces().names();
        for (String keyspaceName : keyspaces)
            calculatePendingRanges(Keyspace.open(keyspaceName).getReplicationStrategy(), keyspaceName);
        if (logger.isTraceEnabled())
            logger.trace("Finished PendingRangeTask for {} keyspaces in {}ms", keyspaces.size(), currentTimeMillis() - start);
        PendingRangeCalculatorServiceDiagnostics.taskFinished();
    });

    public PendingRangeCalculatorService()
    {
    }

    public void update()
    {
        boolean success = update.trigger();
        if (!success) PendingRangeCalculatorServiceDiagnostics.taskRejected(1);
        else PendingRangeCalculatorServiceDiagnostics.taskCountChanged(1);
    }

    public void blockUntilFinished()
    {
        update.sync();
    }


    public void executeWhenFinished(Runnable runnable)
    {
        update.runAfter(runnable);
    }

    // public & static for testing purposes
    public static void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        StorageService.instance.getTokenMetadata().calculatePendingRanges(strategy, keyspaceName);
    }

    @VisibleForTesting
    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }

}
