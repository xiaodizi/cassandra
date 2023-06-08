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
package io.github.xiaodizi.db.marshal;

import java.nio.ByteBuffer;

import io.github.xiaodizi.cql3.CQL3Type;
import io.github.xiaodizi.cql3.Constants;
import io.github.xiaodizi.cql3.Duration;
import io.github.xiaodizi.cql3.Term;
import io.github.xiaodizi.serializers.MarshalException;
import io.github.xiaodizi.serializers.SimpleDateSerializer;
import io.github.xiaodizi.serializers.TypeSerializer;
import io.github.xiaodizi.transport.ProtocolVersion;
import io.github.xiaodizi.utils.ByteBufferUtil;

import static io.github.xiaodizi.cql3.statements.RequestValidations.invalidRequest;

public class SimpleDateType extends TemporalType<Integer>
{
    public static final SimpleDateType instance = new SimpleDateType();

    SimpleDateType() {super(ComparisonType.BYTE_ORDER);} // singleton

    public ByteBuffer fromString(String source) throws MarshalException
    {
        return ByteBufferUtil.bytes(SimpleDateSerializer.dateStringToDays(source));
    }

    @Override
    public ByteBuffer fromTimeInMillis(long millis) throws MarshalException
    {
        return ByteBufferUtil.bytes(SimpleDateSerializer.timeInMillisToDay(millis));
    }

    @Override
    public long toTimeInMillis(ByteBuffer buffer) throws MarshalException
    {
        return SimpleDateSerializer.dayToTimeInMillis(ByteBufferUtil.toInt(buffer));
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return this == otherType || otherType == Int32Type.instance;
    }

    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            return new Constants.Value(fromString((String) parsed));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a string representation of a date value, but got a %s: %s",
                    parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return '"' + SimpleDateSerializer.instance.toString(SimpleDateSerializer.instance.deserialize(buffer)) + '"';
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.DATE;
    }

    public TypeSerializer<Integer> getSerializer()
    {
        return SimpleDateSerializer.instance;
    }

    @Override
    protected void validateDuration(Duration duration)
    {
        // Checks that the duration has no data below days.
        if (!duration.hasDayPrecision())
            throw invalidRequest("The duration must have a day precision. Was: %s", duration);
    }
}
