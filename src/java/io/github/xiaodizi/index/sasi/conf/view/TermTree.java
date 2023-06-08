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
package io.github.xiaodizi.index.sasi.conf.view;

import java.nio.ByteBuffer;
import java.util.Set;

import io.github.xiaodizi.index.sasi.SSTableIndex;
import io.github.xiaodizi.index.sasi.disk.OnDiskIndexBuilder;
import io.github.xiaodizi.index.sasi.plan.Expression;
import io.github.xiaodizi.db.marshal.AbstractType;

public interface TermTree
{
    Set<SSTableIndex> search(Expression e);

    int intervalCount();

    abstract class Builder
    {
        protected final OnDiskIndexBuilder.Mode mode;
        protected final AbstractType<?> comparator;
        protected ByteBuffer min, max;

        protected Builder(OnDiskIndexBuilder.Mode mode, AbstractType<?> comparator)
        {
            this.mode = mode;
            this.comparator = comparator;
        }

        public final void add(SSTableIndex index)
        {
            addIndex(index);

            min = min == null || comparator.compare(min, index.minTerm()) > 0 ? index.minTerm() : min;
            max = max == null || comparator.compare(max, index.maxTerm()) < 0 ? index.maxTerm() : max;
        }

        protected abstract void addIndex(SSTableIndex index);

        public abstract TermTree build();
    }
}
