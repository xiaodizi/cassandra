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
package io.github.xiaodizi.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

import io.github.xiaodizi.cql3.QueryOptions;
import io.github.xiaodizi.cql3.Tuples;
import io.github.xiaodizi.db.TypeSizes;
import io.github.xiaodizi.db.filter.ColumnFilter.Builder;
import io.github.xiaodizi.db.marshal.AbstractType;
import io.github.xiaodizi.db.marshal.TupleType;
import io.github.xiaodizi.exceptions.InvalidRequestException;
import io.github.xiaodizi.io.util.DataInputPlus;
import io.github.xiaodizi.io.util.DataOutputPlus;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.transport.ProtocolVersion;

/**
 * <code>Selector</code> for literal tuples (e.g. (min(value), max(value), count(value))).
 *
 */
final class TupleSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            AbstractType<?> type = readType(metadata, in);
            int size = (int) in.readUnsignedVInt();
            List<Selector> elements = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                elements.add(serializer.deserialize(in, version, metadata));

            return new TupleSelector(type, elements);
        }
    };

    /**
     * The tuple type.
     */
    private final AbstractType<?> type;

    /**
     * The tuple elements
     */
    private final List<Selector> elements;

    public static Factory newFactory(final AbstractType<?> type, final SelectorFactories factories)
    {
        return new CollectionFactory(type, factories)
        {
            protected String getColumnName()
            {
                return Tuples.tupleToString(factories, Factory::getColumnName);
            }

            public Selector newInstance(final QueryOptions options)
            {
                return new TupleSelector(type, factories.newInstances(options));
            }
        };
    }

    @Override
    public void addFetchedColumns(Builder builder)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addFetchedColumns(builder);
    }

    public void addInput(ProtocolVersion protocolVersion, InputRow input)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addInput(protocolVersion, input);
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        ByteBuffer[] buffers = new ByteBuffer[elements.size()];
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            buffers[i] = elements.get(i).getOutput(protocolVersion);
        }
        return TupleType.buildValue(buffers);
    }

    public void reset()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).reset();
    }

    @Override
    public boolean isTerminal()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            if (!elements.get(i).isTerminal())
                return false;
        }
        return true;
    }

    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return Tuples.tupleToString(elements);
    }

    private TupleSelector(AbstractType<?> type, List<Selector> elements)
    {
        super(Kind.TUPLE_SELECTOR);
        this.type = type;
        this.elements = elements;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TupleSelector))
            return false;

        TupleSelector s = (TupleSelector) o;

        return Objects.equal(type, s.type)
            && Objects.equal(elements, s.elements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, elements);
    }

    @Override
    protected int serializedSize(int version)
    {
        int size = sizeOf(type) + TypeSizes.sizeofUnsignedVInt(elements.size());

        for (int i = 0, m = elements.size(); i < m; i++)
            size += serializer.serializedSize(elements.get(i), version);

        return size;
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        writeType(out, type);
        out.writeUnsignedVInt(elements.size());

        for (int i = 0, m = elements.size(); i < m; i++)
            serializer.serialize(elements.get(i), out, version);
    }
}
