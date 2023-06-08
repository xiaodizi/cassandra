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
package io.github.xiaodizi.db;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import io.github.xiaodizi.io.util.File;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.github.xiaodizi.SchemaLoader;
import io.github.xiaodizi.Util;
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.config.ParameterizedClass;
import io.github.xiaodizi.db.rows.UnfilteredRowIterator;
import io.github.xiaodizi.db.commitlog.CommitLog;
import io.github.xiaodizi.exceptions.ConfigurationException;
import io.github.xiaodizi.io.compress.DeflateCompressor;
import io.github.xiaodizi.io.compress.LZ4Compressor;
import io.github.xiaodizi.io.compress.SnappyCompressor;
import io.github.xiaodizi.io.compress.ZstdCompressor;
import io.github.xiaodizi.io.util.FileUtils;
import io.github.xiaodizi.schema.KeyspaceParams;
import io.github.xiaodizi.security.EncryptionContext;
import io.github.xiaodizi.security.EncryptionContextGenerator;

@RunWith(Parameterized.class)
public class RecoveryManagerMissingHeaderTest
{
    private static final String KEYSPACE1 = "RecoveryManager3Test1";
    private static final String CF_STANDARD1 = "Standard1";

    private static final String KEYSPACE2 = "RecoveryManager3Test2";
    private static final String CF_STANDARD3 = "Standard3";

    public RecoveryManagerMissingHeaderTest(ParameterizedClass commitLogCompression, EncryptionContext encryptionContext)
    {
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
        DatabaseDescriptor.setEncryptionContext(encryptionContext);
    }

    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
            {null, EncryptionContextGenerator.createDisabledContext()}, // No compression, no encryption
            {null, EncryptionContextGenerator.createContext(true)}, // Encryption
            {new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()},
            {new ParameterizedClass(SnappyCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()},
            {new ParameterizedClass(DeflateCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()},
            {new ParameterizedClass(ZstdCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()}});
    }

    @Before
    public void setUp() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD3));
    }

    @Test
    public void testMissingHeader() throws IOException
    {
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        Keyspace keyspace2 = Keyspace.open(KEYSPACE2);

        DecoratedKey dk = Util.dk("keymulti");
        UnfilteredRowIterator upd1 = Util.apply(new RowUpdateBuilder(keyspace1.getColumnFamilyStore(CF_STANDARD1).metadata(), 1L, 0, "keymulti")
                                       .clustering("col1").add("val", "1")
                                       .build());

        UnfilteredRowIterator upd2 = Util.apply(new RowUpdateBuilder(keyspace2.getColumnFamilyStore(CF_STANDARD3).metadata(), 1L, 0, "keymulti")
                                       .clustering("col1").add("val", "1")
                                       .build());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

        // nuke the header
        for (File file : new File(DatabaseDescriptor.getCommitLogLocation()).tryList())
        {
            if (file.name().endsWith(".header"))
                FileUtils.deleteWithConfirm(file);
        }

        CommitLog.instance.resetUnsafe(false);

        Assert.assertTrue(Util.sameContent(upd1, Util.getOnlyPartitionUnfiltered(Util.cmd(keyspace1.getColumnFamilyStore(CF_STANDARD1), dk).build()).unfilteredIterator()));
        Assert.assertTrue(Util.sameContent(upd2, Util.getOnlyPartitionUnfiltered(Util.cmd(keyspace2.getColumnFamilyStore(CF_STANDARD3), dk).build()).unfilteredIterator()));
    }
}
