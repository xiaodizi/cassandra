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
package io.github.xiaodizi.index.sasi.memory;

import java.nio.ByteBuffer;

import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.index.sasi.conf.ColumnIndex;
import io.github.xiaodizi.index.sasi.disk.Token;
import io.github.xiaodizi.index.sasi.plan.Expression;
import io.github.xiaodizi.index.sasi.utils.RangeIterator;
import io.github.xiaodizi.db.marshal.AbstractType;

public abstract class MemIndex
{
    protected final AbstractType<?> keyValidator;
    protected final ColumnIndex columnIndex;

    protected MemIndex(AbstractType<?> keyValidator, ColumnIndex columnIndex)
    {
        this.keyValidator = keyValidator;
        this.columnIndex = columnIndex;
    }

    public abstract long add(DecoratedKey key, ByteBuffer value);
    public abstract RangeIterator<Long, Token> search(Expression expression);

    public static MemIndex forColumn(AbstractType<?> keyValidator, ColumnIndex columnIndex)
    {
        return columnIndex.isLiteral()
                ? new TrieMemIndex(keyValidator, columnIndex)
                : new SkipListMemIndex(keyValidator, columnIndex);
    }
}
