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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.marshal.BytesType;
import io.github.xiaodizi.db.marshal.DecimalType;
import io.github.xiaodizi.db.marshal.DoubleType;
import io.github.xiaodizi.db.marshal.FloatType;
import io.github.xiaodizi.db.marshal.InetAddressType;
import io.github.xiaodizi.db.marshal.Int32Type;
import io.github.xiaodizi.db.marshal.LongType;
import io.github.xiaodizi.db.marshal.ShortType;
import io.github.xiaodizi.db.marshal.SimpleDateType;
import io.github.xiaodizi.db.marshal.TimeType;
import io.github.xiaodizi.db.marshal.TimeUUIDType;
import io.github.xiaodizi.db.marshal.TimestampType;
import io.github.xiaodizi.db.marshal.UTF8Type;
import io.github.xiaodizi.db.marshal.UUIDType;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.transport.ProtocolVersion;
import io.github.xiaodizi.utils.ByteBufferUtil;

import static org.junit.Assume.assumeTrue;

public class EmptyValuesTest extends CQLTester
{
    private void verify(String emptyValue) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s");
        UntypedResultSet.Row row = result.one();
        Assert.assertTrue(row.getColumns().stream().anyMatch(c -> c.name.toString().equals("v")));
        Assert.assertEquals(0, row.getBytes("v").remaining());

        ResultSet resultNet = executeNet(ProtocolVersion.CURRENT, "SELECT * FROM %s");
        Row rowNet = resultNet.one();
        Assert.assertTrue(rowNet.getColumnDefinitions().contains("v"));
        Assert.assertEquals(0, rowNet.getBytesUnsafe("v").remaining());

        ResultSet jsonNet = executeNet(ProtocolVersion.CURRENT, "SELECT JSON * FROM %s");
        Row jsonRowNet = jsonNet.one();
        Assert.assertTrue(jsonRowNet.getString("[json]"), jsonRowNet.getString("[json]").matches(".*\"v\"\\s*:\\s*\"" + Pattern.quote(emptyValue) + "\".*"));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        for (SSTableReader ssTable : cfs.getLiveSSTables())
        {
            try (PrintStream out = new PrintStream(buf, true))
            {
                ProcessBuilder pb = new ProcessBuilder("tools/bin/sstabledump", ssTable.getFilename());
                Process process = pb.start();
                process.waitFor();
                IOUtils.copy(process.getInputStream(), buf);
            }
            catch (Throwable t)
            {
                Assert.fail(t.getClass().getName());
            }
        }
        
        String outString = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertTrue(outString, outString.contains("{ \"name\" : \"v\", \"value\" : \"" + emptyValue + "\" }"));
    }

    private void verifyPlainInsert(String emptyValue) throws Throwable
    {
        execute("TRUNCATE %s");

        // In most cases we cannot insert empty value when we do not bind variables
        // This is due to the current implementation of io.github.xiaodizi.cql3.Constants.Literal.testAssignment
        // execute("INSERT INTO %s (id, v) VALUES (1, '" + emptyValue + "')");
        execute("INSERT INTO %s (id, v) VALUES (1, ?)", ByteBufferUtil.EMPTY_BYTE_BUFFER);
        flush();

        verify(emptyValue);
    }

    private void verifyJsonInsert(String emptyValue) throws Throwable
    {
        execute("TRUNCATE %s");
        execute("INSERT INTO %s JSON '{\"id\":\"1\",\"v\":\"" + emptyValue + "\"}'");
        flush();

        verify(emptyValue);
    }

    @Test
    public void testEmptyInt() throws Throwable
    {
        assumeTrue(Int32Type.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v INT)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyText() throws Throwable
    {
        assumeTrue(UTF8Type.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v TEXT)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyTimestamp() throws Throwable
    {
        assumeTrue(TimestampType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v TIMESTAMP)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyUUID() throws Throwable
    {
        assumeTrue(UUIDType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v UUID)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyInetAddress() throws Throwable
    {
        assumeTrue(InetAddressType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v INET)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyLong() throws Throwable
    {
        assumeTrue(LongType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v BIGINT)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyBytes() throws Throwable
    {
        assumeTrue(BytesType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v BLOB)");
        verifyJsonInsert("0x");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyDate() throws Throwable
    {
        assumeTrue(SimpleDateType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v DATE)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyDecimal() throws Throwable
    {
        assumeTrue(DecimalType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v DECIMAL)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyDouble() throws Throwable
    {
        assumeTrue(DoubleType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v DOUBLE)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyFloat() throws Throwable
    {
        assumeTrue(FloatType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v FLOAT)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptySmallInt() throws Throwable
    {
        assumeTrue(ShortType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v SMALLINT)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyTime() throws Throwable
    {
        assumeTrue(TimeType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v TIME)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }

    @Test
    public void testEmptyTimeUUID() throws Throwable
    {
        assumeTrue(TimeUUIDType.instance.isEmptyValueMeaningless());
        String table = createTable("CREATE TABLE %s (id INT PRIMARY KEY, v TIMEUUID)");
        verifyJsonInsert("");
        verifyPlainInsert("");
    }
}
