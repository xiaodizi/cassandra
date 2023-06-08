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
import java.util.*;
import java.util.stream.Collectors;

import io.github.xiaodizi.index.sasi.SSTableIndex;
import io.github.xiaodizi.index.sasi.conf.ColumnIndex;
import io.github.xiaodizi.index.sasi.plan.Expression;
import io.github.xiaodizi.db.marshal.AbstractType;
import io.github.xiaodizi.db.marshal.AsciiType;
import io.github.xiaodizi.db.marshal.UTF8Type;
import io.github.xiaodizi.io.sstable.Descriptor;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.utils.Interval;
import io.github.xiaodizi.utils.IntervalTree;

import com.google.common.collect.Iterables;

public class View implements Iterable<SSTableIndex>
{
    private final Map<Descriptor, SSTableIndex> view;

    private final TermTree termTree;
    private final AbstractType<?> keyValidator;
    private final IntervalTree<Key, SSTableIndex, Interval<Key, SSTableIndex>> keyIntervalTree;

    public View(ColumnIndex index, Set<SSTableIndex> indexes)
    {
        this(index, Collections.<SSTableIndex>emptyList(), Collections.<SSTableReader>emptyList(), indexes);
    }

    public View(ColumnIndex index,
                Collection<SSTableIndex> currentView,
                Collection<SSTableReader> oldSSTables,
                Set<SSTableIndex> newIndexes)
    {
        Map<Descriptor, SSTableIndex> newView = new HashMap<>();

        AbstractType<?> validator = index.getValidator();
        TermTree.Builder termTreeBuilder = (validator instanceof AsciiType || validator instanceof UTF8Type)
                                            ? new PrefixTermTree.Builder(index.getMode().mode, validator)
                                            : new RangeTermTree.Builder(index.getMode().mode, validator);

        List<Interval<Key, SSTableIndex>> keyIntervals = new ArrayList<>();
        // Ensure oldSSTables and newIndexes are disjoint (in index redistribution case the intersection can be non-empty).
        // also favor newIndexes over currentView in case an SSTable has been re-opened (also occurs during redistribution)
        // See CASSANDRA-14055
        Collection<SSTableReader> toRemove = new HashSet<>(oldSSTables);
        toRemove.removeAll(newIndexes.stream().map(SSTableIndex::getSSTable).collect(Collectors.toSet()));
        for (SSTableIndex sstableIndex : Iterables.concat(newIndexes, currentView))
        {
            SSTableReader sstable = sstableIndex.getSSTable();
            if (toRemove.contains(sstable) || sstable.isMarkedCompacted() || newView.containsKey(sstable.descriptor))
            {
                sstableIndex.release();
                continue;
            }

            newView.put(sstable.descriptor, sstableIndex);

            termTreeBuilder.add(sstableIndex);
            keyIntervals.add(Interval.create(new Key(sstableIndex.minKey(), index.keyValidator()),
                                             new Key(sstableIndex.maxKey(), index.keyValidator()),
                                             sstableIndex));
        }

        this.view = newView;
        this.termTree = termTreeBuilder.build();
        this.keyValidator = index.keyValidator();
        this.keyIntervalTree = IntervalTree.build(keyIntervals);

        if (keyIntervalTree.intervalCount() != termTree.intervalCount())
            throw new IllegalStateException(String.format("mismatched sizes for intervals tree for keys vs terms: %d != %d", keyIntervalTree.intervalCount(), termTree.intervalCount()));
    }

    public Set<SSTableIndex> match(Expression expression)
    {
        return termTree.search(expression);
    }

    public List<SSTableIndex> match(ByteBuffer minKey, ByteBuffer maxKey)
    {
        return keyIntervalTree.search(Interval.create(new Key(minKey, keyValidator), new Key(maxKey, keyValidator), (SSTableIndex) null));
    }

    public Iterator<SSTableIndex> iterator()
    {
        return view.values().iterator();
    }

    public Collection<SSTableIndex> getIndexes()
    {
        return view.values();
    }

    /**
     * This is required since IntervalTree doesn't support custom Comparator
     * implementations and relied on items to be comparable which "raw" keys are not.
     */
    private static class Key implements Comparable<Key>
    {
        private final ByteBuffer key;
        private final AbstractType<?> comparator;

        public Key(ByteBuffer key, AbstractType<?> comparator)
        {
            this.key = key;
            this.comparator = comparator;
        }

        public int compareTo(Key o)
        {
            return comparator.compare(key, o.key);
        }
    }
}
