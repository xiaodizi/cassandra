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
package io.github.xiaodizi.test.microbench;

import io.github.xiaodizi.metrics.CassandraMetricsRegistry;
import io.github.xiaodizi.metrics.DecayingEstimatedHistogramReservoir;
import io.github.xiaodizi.metrics.LatencyMetrics;
import io.github.xiaodizi.metrics.MetricNameFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class LatencyTrackingBench
{
    private LatencyMetrics metrics;
    private LatencyMetrics parent;
    private LatencyMetrics grandParent;
    private DecayingEstimatedHistogramReservoir dehr;
    private final MetricNameFactory factory = new BenchMetricsNameFactory();
    private long[] values = new long[1024];

    class BenchMetricsNameFactory implements MetricNameFactory
    {

        @Override
        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            return new CassandraMetricsRegistry.MetricName(BenchMetricsNameFactory.class, metricName);
        }
    }

    @Setup(Level.Iteration)
    public void setup() 
    {
        parent = new LatencyMetrics("test", "testCF");
        grandParent = new LatencyMetrics("test", "testCF");

        // Replicates behavior from ColumnFamilyStore metrics
        metrics = new LatencyMetrics(factory, "testCF", parent, grandParent);
        dehr = new DecayingEstimatedHistogramReservoir(false);
        for(int i = 0; i < 1024; i++) 
        {
            values[i] = TimeUnit.MICROSECONDS.toNanos(ThreadLocalRandom.current().nextLong(346));
        }
    }

    @Setup(Level.Invocation)
    public void reset() 
    {
        dehr = new DecayingEstimatedHistogramReservoir(false);
        metrics.release();
        metrics = new LatencyMetrics(factory, "testCF", parent, grandParent);
    }

    @Benchmark
    @OperationsPerInvocation(1024)
    public void benchLatencyMetricsWrite() 
    {
        for(int i = 0; i < values.length; i++) 
        {
            metrics.addNano(values[i]);
        }
    }

    @Benchmark
    @OperationsPerInvocation(1024)
    public void benchInsertToDEHR(Blackhole bh) 
    {
        for(int i = 0; i < values.length; i++) 
        {
            dehr.update(values[i]);
        }
        bh.consume(dehr);
    }
}
