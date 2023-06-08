/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.github.xiaodizi.utils.memory;

import java.nio.ByteBuffer;

import io.github.xiaodizi.db.BufferDecoratedKey;
import io.github.xiaodizi.db.Clustering;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.marshal.ByteArrayAccessor;
import io.github.xiaodizi.db.marshal.ByteBufferAccessor;
import io.github.xiaodizi.db.marshal.ValueAccessor;
import io.github.xiaodizi.db.rows.Cell;
import io.github.xiaodizi.utils.ByteBufferUtil;

/**
 * Cloner class that can be use to clone partition elements using on-heap or off-heap buffers.
 *
 */
public abstract class ByteBufferCloner implements Cloner
{
    /**
     * Allocate a slice of the given length.
     */
    public abstract ByteBuffer allocate(int size);

    @Override
    public DecoratedKey clone(DecoratedKey key)
    {
        return new BufferDecoratedKey(key.getToken(), clone(key.getKey()));
    }

    @Override
    public Clustering<?> clone(Clustering<?> clustering)
    {
        return clustering.clone(this);
    }

    @Override
    public Cell<?> clone(Cell<?> cell)
    {
        return cell.clone(this);
    }

    public final ByteBuffer clone(ByteBuffer buffer)
    {
        return clone(buffer, ByteBufferAccessor.instance);
    }

    public final ByteBuffer clone(byte[] bytes)
    {
        return clone(bytes, ByteArrayAccessor.instance);
    }

    public final <V> ByteBuffer clone(V value, ValueAccessor<V> accessor)
    {
        assert value != null;
        int size = accessor.size(value);
        if (size == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        ByteBuffer cloned = allocate(size);

        cloned.mark();
        accessor.write(value, cloned);
        cloned.reset();
        return cloned;
    }
}