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
package io.github.xiaodizi.index.sasi.utils;

import java.nio.ByteBuffer;

import io.github.xiaodizi.index.sasi.disk.Descriptor;
import io.github.xiaodizi.index.sasi.disk.OnDiskIndex;
import io.github.xiaodizi.index.sasi.disk.TokenTreeBuilder;
import io.github.xiaodizi.index.sasi.sa.IndexedTerm;
import io.github.xiaodizi.index.sasi.sa.TermIterator;
import io.github.xiaodizi.db.marshal.AbstractType;
import io.github.xiaodizi.utils.Pair;

@SuppressWarnings("resource")
public class CombinedTermIterator extends TermIterator
{
    final Descriptor descriptor;
    final RangeIterator<OnDiskIndex.DataTerm, CombinedTerm> union;
    final ByteBuffer min;
    final ByteBuffer max;

    public CombinedTermIterator(OnDiskIndex... sas)
    {
        this(Descriptor.CURRENT, sas);
    }

    public CombinedTermIterator(Descriptor d, OnDiskIndex... parts)
    {
        descriptor = d;
        union = OnDiskIndexIterator.union(parts);

        AbstractType<?> comparator = parts[0].getComparator(); // assumes all SAs have same comparator
        ByteBuffer minimum = parts[0].minTerm();
        ByteBuffer maximum = parts[0].maxTerm();

        for (int i = 1; i < parts.length; i++)
        {
            OnDiskIndex part = parts[i];
            if (part == null)
                continue;

            minimum = comparator.compare(minimum, part.minTerm()) > 0 ? part.minTerm() : minimum;
            maximum = comparator.compare(maximum, part.maxTerm()) < 0 ? part.maxTerm() : maximum;
        }

        min = minimum;
        max = maximum;
    }

    public ByteBuffer minTerm()
    {
        return min;
    }

    public ByteBuffer maxTerm()
    {
        return max;
    }

    protected Pair<IndexedTerm, TokenTreeBuilder> computeNext()
    {
        if (!union.hasNext())
        {
            return endOfData();
        }
        else
        {
            CombinedTerm term = union.next();
            return Pair.create(new IndexedTerm(term.getTerm(), term.isPartial()), term.getTokenTreeBuilder());
        }

    }
}
