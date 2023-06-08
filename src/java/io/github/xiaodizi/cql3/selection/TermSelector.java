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

import com.google.common.base.Objects;

import io.github.xiaodizi.schema.ColumnMetadata;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.cql3.ColumnSpecification;
import io.github.xiaodizi.cql3.QueryOptions;
import io.github.xiaodizi.cql3.Term;
import io.github.xiaodizi.db.filter.ColumnFilter;
import io.github.xiaodizi.db.marshal.AbstractType;
import io.github.xiaodizi.io.util.DataInputPlus;
import io.github.xiaodizi.io.util.DataOutputPlus;
import io.github.xiaodizi.transport.ProtocolVersion;
import io.github.xiaodizi.utils.ByteBufferUtil;

/**
 * Selector representing a simple term (literals or bound variables).
 * <p>
 * Note that we know the term does not include function calls for instance (this is actually enforced by the parser), those
 * being dealt with by their own Selector.
 */
public class TermSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            AbstractType<?> type = readType(metadata, in);
            ByteBuffer value = ByteBufferUtil.readWithVIntLength(in);
            return new TermSelector(value, type);
        }
    };

    private final ByteBuffer value;
    private final AbstractType<?> type;

    public static Factory newFactory(final String name, final Term term, final AbstractType<?> type)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return name;
            }

            protected AbstractType<?> getReturnType()
            {
                return type;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultColumn)
            {
               mapping.addMapping(resultColumn, (ColumnMetadata)null);
            }

            public Selector newInstance(QueryOptions options)
            {
                return new TermSelector(term.bindAndGet(options), type);
            }

            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
            }

            public boolean areAllFetchedColumnsKnown()
            {
                return true;
            }
        };
    }

    TermSelector(ByteBuffer value, AbstractType<?> type)
    {
        super(Kind.TERM_SELECTOR);
        this.value = value;
        this.type = type;
    }

    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
    }

    public void addInput(ProtocolVersion protocolVersion, InputRow input)
    {
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        return value;
    }

    public AbstractType<?> getType()
    {
        return type;
    }

    public void reset()
    {
    }

    @Override
    public boolean isTerminal()
    {
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TermSelector))
            return false;

        TermSelector s = (TermSelector) o;

        return Objects.equal(value, s.value)
            && Objects.equal(type, s.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(value, type);
    }

    @Override
    protected int serializedSize(int version)
    {
        return sizeOf(type) + ByteBufferUtil.serializedSizeWithVIntLength(value);
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        writeType(out, type);
        ByteBufferUtil.writeWithVIntLength(value, out);
    }
}
