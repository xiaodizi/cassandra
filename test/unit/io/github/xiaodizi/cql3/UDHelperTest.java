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

package io.github.xiaodizi.cql3;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import io.github.xiaodizi.cql3.functions.UDHelper;
import io.github.xiaodizi.db.marshal.AbstractType;
import io.github.xiaodizi.db.marshal.AsciiType;
import io.github.xiaodizi.db.marshal.BooleanType;
import io.github.xiaodizi.db.marshal.ByteType;
import io.github.xiaodizi.db.marshal.BytesType;
import io.github.xiaodizi.db.marshal.CounterColumnType;
import io.github.xiaodizi.db.marshal.DateType;
import io.github.xiaodizi.db.marshal.DecimalType;
import io.github.xiaodizi.db.marshal.DoubleType;
import io.github.xiaodizi.db.marshal.FloatType;
import io.github.xiaodizi.db.marshal.InetAddressType;
import io.github.xiaodizi.db.marshal.Int32Type;
import io.github.xiaodizi.db.marshal.IntegerType;
import io.github.xiaodizi.db.marshal.LongType;
import io.github.xiaodizi.db.marshal.ReversedType;
import io.github.xiaodizi.db.marshal.ShortType;
import io.github.xiaodizi.db.marshal.SimpleDateType;
import io.github.xiaodizi.db.marshal.TimeType;
import io.github.xiaodizi.db.marshal.TimeUUIDType;
import io.github.xiaodizi.db.marshal.TimestampType;
import io.github.xiaodizi.db.marshal.UTF8Type;
import io.github.xiaodizi.db.marshal.UUIDType;
import io.github.xiaodizi.db.marshal.ValueAccessor;
import io.github.xiaodizi.serializers.MarshalException;
import io.github.xiaodizi.serializers.TypeSerializer;
import io.github.xiaodizi.utils.ByteBufferUtil;

public class UDHelperTest
{
    static class UFTestCustomType extends AbstractType<String>
    {
        protected UFTestCustomType()
        {
            super(ComparisonType.CUSTOM);
        }

        public ByteBuffer fromString(String source) throws MarshalException
        {
            return ByteBuffer.wrap(source.getBytes());
        }

        public Term fromJSONObject(Object parsed) throws MarshalException
        {
            throw new UnsupportedOperationException();
        }

        public TypeSerializer<String> getSerializer()
        {
            return UTF8Type.instance.getSerializer();
        }

        public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
        {
            return ValueAccessor.compare(left, accessorL, right, accessorR);
        }
    }

    @Test
    public void testEmptyVariableLengthTypes()
    {
        AbstractType<?>[] types = new AbstractType<?>[]{
                                                       AsciiType.instance,
                                                       BytesType.instance,
                                                       UTF8Type.instance,
                                                       new UFTestCustomType()
        };

        for (AbstractType<?> type : types)
        {
            Assert.assertFalse("type " + type.getClass().getName(),
                               UDHelper.isNullOrEmpty(type, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }
    }

    @Test
    public void testNonEmptyPrimitiveTypes()
    {
        AbstractType<?>[] types = new AbstractType<?>[]{
                                                       TimeType.instance,
                                                       SimpleDateType.instance,
                                                       ByteType.instance,
                                                       ShortType.instance
        };

        for (AbstractType<?> type : types)
        {
            try
            {
                type.getSerializer().validate(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                Assert.fail(type.getClass().getSimpleName());
            }
            catch (MarshalException e)
            {
                //
            }
        }
    }

    @Test
    public void testEmptiableTypes()
    {
        AbstractType<?>[] types = new AbstractType<?>[]{
                                                       BooleanType.instance,
                                                       CounterColumnType.instance,
                                                       DateType.instance,
                                                       DecimalType.instance,
                                                       DoubleType.instance,
                                                       FloatType.instance,
                                                       InetAddressType.instance,
                                                       Int32Type.instance,
                                                       IntegerType.instance,
                                                       LongType.instance,
                                                       TimestampType.instance,
                                                       TimeUUIDType.instance,
                                                       UUIDType.instance
        };

        for (AbstractType<?> type : types)
        {
            Assert.assertTrue(type.getClass().getSimpleName(), UDHelper.isNullOrEmpty(type, ByteBufferUtil.EMPTY_BYTE_BUFFER));
            Assert.assertTrue("reversed " + type.getClass().getSimpleName(),
                              UDHelper.isNullOrEmpty(ReversedType.getInstance(type), ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }
    }
}
