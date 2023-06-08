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

package io.github.xiaodizi.simulator.systems;

import io.github.xiaodizi.simulator.RandomSource;
import io.github.xiaodizi.simulator.systems.InterceptedWait.CaptureSites.Capture;
import io.github.xiaodizi.simulator.systems.InterceptedWait.InterceptedConditionWait;
import io.github.xiaodizi.utils.Clock;
import io.github.xiaodizi.utils.concurrent.Condition;
import io.github.xiaodizi.utils.concurrent.CountDownLatch;
import io.github.xiaodizi.utils.concurrent.WaitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static io.github.xiaodizi.config.CassandraRelevantProperties.TEST_SIMULATOR_DETERMINISM_CHECK;
import static io.github.xiaodizi.simulator.SimulatorUtils.failWithOOM;
import static io.github.xiaodizi.simulator.systems.InterceptedWait.Kind.NEMESIS;
import static io.github.xiaodizi.simulator.systems.NonInterceptible.Permit.OPTIONAL;
import static io.github.xiaodizi.simulator.systems.NonInterceptible.Permit.REQUIRED;

@PerClassLoader
public class InterceptingGlobalMethods extends InterceptingMonitors implements InterceptorOfGlobalMethods
{
    private static final Logger logger = LoggerFactory.getLogger(InterceptingGlobalMethods.class);
    private static final boolean isDeterminismCheckStrict = TEST_SIMULATOR_DETERMINISM_CHECK.convert(name -> name.equals("strict"));

    private final @Nullable LongConsumer onThreadLocalRandomCheck;
    private final Capture capture;
    private int uniqueUuidCounter = 0;
    private final Consumer<Throwable> onUncaughtException;

    public InterceptingGlobalMethods(Capture capture, LongConsumer onThreadLocalRandomCheck, Consumer<Throwable> onUncaughtException, RandomSource random)
    {
        super(random);
        this.capture = capture.any() ? capture : null;
        this.onThreadLocalRandomCheck = onThreadLocalRandomCheck;
        this.onUncaughtException = onUncaughtException;
    }

    @Override
    public WaitQueue newWaitQueue()
    {
        return new InterceptingWaitQueue();
    }

    @Override
    public CountDownLatch newCountDownLatch(int count)
    {
        return new InterceptingAwaitable.InterceptingCountDownLatch(count);
    }

    @Override
    public Condition newOneTimeCondition()
    {
        return new InterceptingAwaitable.InterceptingCondition();
    }

    @Override
    public InterceptedWait.CaptureSites captureWaitSite(Thread thread)
    {
        if (capture == null)
            return null;

        return new InterceptedWait.CaptureSites(thread, capture);
    }

    @Override
    public InterceptibleThread ifIntercepted()
    {
        Thread thread = Thread.currentThread();
        if (thread instanceof InterceptibleThread)
        {
            InterceptibleThread interceptibleThread = (InterceptibleThread) thread;
            if (interceptibleThread.isIntercepting())
                return interceptibleThread;
        }

        if (NonInterceptible.isPermitted())
            return null;

        if (!disabled)
            throw failWithOOM();

        return null;
    }

    @Override
    public void uncaughtException(Thread thread, Throwable throwable)
    {
        onUncaughtException.accept(throwable);
    }

    @Override
    public void nemesis(float chance)
    {
        InterceptibleThread thread = ifIntercepted();
        if (thread == null || thread.isEvaluationDeterministic() || !random.decide(chance))
            return;

        InterceptedConditionWait signal = new InterceptedConditionWait(NEMESIS, 0L, thread, captureWaitSite(thread), null);
        thread.interceptWait(signal);

        // save interrupt state to restore afterwards - new ones only arrive if terminating simulation
        boolean restoreInterrupt = Thread.interrupted();
        try
        {
            while (true)
            {
                try
                {
                    signal.awaitDeclaredUninterruptible();
                    return;
                }
                catch (InterruptedException e)
                {
                    restoreInterrupt = true;
                    if (disabled)
                        return;
                }
            }
        }
        finally
        {
            if (restoreInterrupt)
                thread.interrupt();
        }
    }

    @Override
    public long randomSeed()
    {
        InterceptibleThread thread = ifIntercepted();
        if (thread == null || thread.isEvaluationDeterministic())
            return Thread.currentThread().getName().hashCode();

        return random.uniform(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Override
    public synchronized UUID randomUUID()
    {
        long msb = random.uniform(0, 1L << 60);
        msb = ((msb << 4) & 0xffffffffffff0000L) | 0x4000 | (msb & 0xfff);
        return new UUID(msb, (1L << 63) | uniqueUuidCounter++);
    }

    @Override
    public void threadLocalRandomCheck(long seed)
    {
        if (onThreadLocalRandomCheck != null)
            onThreadLocalRandomCheck.accept(seed);
    }

    public static class ThreadLocalRandomCheck implements LongConsumer
    {
        final LongConsumer wrapped;
        private boolean disabled;

        public ThreadLocalRandomCheck(LongConsumer wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public void accept(long value)
        {
            if (wrapped != null)
                wrapped.accept(value);

            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                InterceptibleThread interceptibleThread = (InterceptibleThread) thread;
                if (interceptibleThread.isIntercepting())
                    return;
            }

            if (NonInterceptible.isPermitted(isDeterminismCheckStrict ? OPTIONAL : REQUIRED))
                return;

            if (!disabled)
                throw failWithOOM();
        }

        public void stop()
        {
            disabled = true;
        }
    }

    @Override
    public long nanoTime()
    {
        return Clock.Global.nanoTime();
    }

    @Override
    public long currentTimeMillis()
    {
        return Clock.Global.currentTimeMillis();
    }
}
