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

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 6, time = 20)
@Fork(value = 1, jvmArgsAppend = { "-Xmx256M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class AutoBoxingBench
{

    @Benchmark
    public boolean booleanFromBooleanSupplier()
    {
        BooleanSupplier bs = () -> true;
        return bs.getAsBoolean();
    }

    @Benchmark
    public boolean booleanFromPlainSupplier()
    {
        Supplier<Boolean> bs = () -> true;
        return bs.get();
    }

    @Benchmark
    public int intFromIntSupplier()
    {
        IntSupplier bs = () -> 42;
        return bs.getAsInt();
    }

    @Benchmark
    public int intFromPlainSupplier()
    {
        Supplier<Integer> bs = () -> 42;
        return bs.get();
    }
}
