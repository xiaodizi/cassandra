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

package io.github.xiaodizi.stress.operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.github.xiaodizi.stress.Operation;
import io.github.xiaodizi.stress.WorkManager;
import io.github.xiaodizi.stress.generate.Distribution;
import io.github.xiaodizi.stress.generate.PartitionGenerator;
import io.github.xiaodizi.stress.generate.PartitionIterator;
import io.github.xiaodizi.stress.generate.RatioDistribution;
import io.github.xiaodizi.stress.generate.Seed;
import io.github.xiaodizi.stress.generate.SeedManager;
import io.github.xiaodizi.stress.report.Timer;
import io.github.xiaodizi.stress.settings.OptionRatioDistribution;
import io.github.xiaodizi.stress.settings.StressSettings;

public abstract class PartitionOperation extends Operation
{
    protected final DataSpec spec;
    private final static RatioDistribution defaultRowPopulationRatio = OptionRatioDistribution.BUILDER.apply("fixed(1)/1").get();

    private final List<PartitionIterator> partitionCache = new ArrayList<>();
    protected List<PartitionIterator> partitions;

    public static final class DataSpec
    {
        public final PartitionGenerator partitionGenerator;
        final SeedManager seedManager;
        final Distribution partitionCount;
        final RatioDistribution useRatio;
        final RatioDistribution rowPopulationRatio;
        final Integer targetCount;

        public DataSpec(PartitionGenerator partitionGenerator, SeedManager seedManager, Distribution partitionCount, RatioDistribution rowPopulationRatio, Integer targetCount)
        {
            this(partitionGenerator, seedManager, partitionCount, null, rowPopulationRatio, targetCount);
        }
        public DataSpec(PartitionGenerator partitionGenerator, SeedManager seedManager, Distribution partitionCount, RatioDistribution useRatio, RatioDistribution rowPopulationRatio)
        {
            this(partitionGenerator, seedManager, partitionCount, useRatio, rowPopulationRatio, null);
        }
        private DataSpec(PartitionGenerator partitionGenerator, SeedManager seedManager, Distribution partitionCount, RatioDistribution useRatio, RatioDistribution rowPopulationRatio, Integer targetCount)
        {
            this.partitionGenerator = partitionGenerator;
            this.seedManager = seedManager;
            this.partitionCount = partitionCount;
            this.useRatio = useRatio;
            this.rowPopulationRatio = rowPopulationRatio == null ? defaultRowPopulationRatio : rowPopulationRatio;
            this.targetCount = targetCount;
        }
    }

    public PartitionOperation(Timer timer, StressSettings settings, DataSpec spec)
    {
        super(timer, settings);
        this.spec = spec;
    }

    public DataSpec getDataSpecification()
    {
        return spec;
    }

    public List<PartitionIterator> getPartitions()
    {
        return Collections.unmodifiableList(partitions);
    }

    public int ready(WorkManager permits)
    {
        int partitionCount = (int) spec.partitionCount.next();
        if (partitionCount <= 0)
            return 0;
        partitionCount = permits.takePermits(partitionCount);
        if (partitionCount <= 0)
            return 0;

        int i = 0;
        boolean success = true;
        for (; i < partitionCount && success; i++)
        {
            if (i >= partitionCache.size())
                partitionCache.add(PartitionIterator.get(spec.partitionGenerator, spec.seedManager));

            success = false;
            while (!success)
            {
                Seed seed = spec.seedManager.next(this);
                if (seed == null)
                    break;

                success = reset(seed, partitionCache.get(i));
            }
        }
        partitionCount = i;

        partitions = partitionCache.subList(0, partitionCount);
        return partitions.size();
    }

    protected boolean reset(Seed seed, PartitionIterator iterator)
    {
        if (spec.useRatio == null)
            return iterator.reset(seed, spec.targetCount, spec.rowPopulationRatio.next(), isWrite());
        else
            return iterator.reset(seed, spec.useRatio.next(), spec.rowPopulationRatio.next(), isWrite());
    }

    public String key()
    {
        List<String> keys = new ArrayList<>();
        for (PartitionIterator partition : partitions)
            keys.add(partition.getKeyAsString());
        return keys.toString();
    }
}
