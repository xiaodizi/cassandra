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

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import com.google.common.base.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import io.github.xiaodizi.UpdateBuilder;
import io.github.xiaodizi.Util;
import io.github.xiaodizi.batchlog.Batch;
import io.github.xiaodizi.batchlog.BatchlogManager;
import io.github.xiaodizi.cache.ChunkCache;
import io.github.xiaodizi.db.compaction.CompactionManager;
import io.github.xiaodizi.db.compaction.Verifier;
import io.github.xiaodizi.db.marshal.UUIDType;
import io.github.xiaodizi.dht.ByteOrderedPartitioner;
import io.github.xiaodizi.dht.Murmur3Partitioner;
import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.exceptions.ConfigurationException;
import io.github.xiaodizi.exceptions.WriteTimeoutException;
import io.github.xiaodizi.io.FSWriteError;
import io.github.xiaodizi.io.sstable.Component;
import io.github.xiaodizi.io.sstable.CorruptSSTableException;
import io.github.xiaodizi.io.sstable.format.SSTableReader;
import io.github.xiaodizi.io.util.File;
import io.github.xiaodizi.io.util.FileInputStreamPlus;
import io.github.xiaodizi.io.util.FileUtils;
import io.github.xiaodizi.io.util.RandomAccessReader;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.locator.TokenMetadata;
import io.github.xiaodizi.schema.CompressionParams;
import io.github.xiaodizi.schema.KeyspaceParams;
import io.github.xiaodizi.service.StorageService;
import io.github.xiaodizi.utils.ByteBufferUtil;

import static io.github.xiaodizi.SchemaLoader.counterCFMD;
import static io.github.xiaodizi.SchemaLoader.createKeyspace;
import static io.github.xiaodizi.SchemaLoader.loadSchema;
import static io.github.xiaodizi.SchemaLoader.standardCFMD;
import static io.github.xiaodizi.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link Verifier}.
 * 
 * Note: the complete coverage is composed of:
 * - {@link io.github.xiaodizi.tools.StandaloneVerifierOnSSTablesTest}
 * - {@link io.github.xiaodizi.tools.StandaloneVerifierTest}
 * - {@link VerifyTest}
 */
public class VerifyTest
{
    public static final String KEYSPACE = "Keyspace1";
    public static final String CF = "Standard1";
    public static final String CF2 = "Standard2";
    public static final String CF3 = "Standard3";
    public static final String CF4 = "Standard4";
    public static final String COUNTER_CF = "Counter1";
    public static final String COUNTER_CF2 = "Counter2";
    public static final String COUNTER_CF3 = "Counter3";
    public static final String COUNTER_CF4 = "Counter4";
    public static final String CORRUPT_CF = "Corrupt1";
    public static final String CORRUPT_CF2 = "Corrupt2";
    public static final String CORRUPTCOUNTER_CF = "CounterCorrupt1";
    public static final String CORRUPTCOUNTER_CF2 = "CounterCorrupt2";

    public static final String CF_UUID = "UUIDKeys";
    public static final String BF_ALWAYS_PRESENT = "BfAlwaysPresent";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CompressionParams compressionParameters = CompressionParams.snappy(32768);

        loadSchema();
        createKeyspace(KEYSPACE,
                       KeyspaceParams.simple(1),
                       standardCFMD(KEYSPACE, CF).compression(compressionParameters),
                       standardCFMD(KEYSPACE, CF2).compression(compressionParameters),
                       standardCFMD(KEYSPACE, CF3),
                       standardCFMD(KEYSPACE, CF4),
                       standardCFMD(KEYSPACE, CORRUPT_CF),
                       standardCFMD(KEYSPACE, CORRUPT_CF2),
                       counterCFMD(KEYSPACE, COUNTER_CF).compression(compressionParameters),
                       counterCFMD(KEYSPACE, COUNTER_CF2).compression(compressionParameters),
                       counterCFMD(KEYSPACE, COUNTER_CF3),
                       counterCFMD(KEYSPACE, COUNTER_CF4),
                       counterCFMD(KEYSPACE, CORRUPTCOUNTER_CF),
                       counterCFMD(KEYSPACE, CORRUPTCOUNTER_CF2),
                       standardCFMD(KEYSPACE, CF_UUID, 0, UUIDType.instance),
                       standardCFMD(KEYSPACE, BF_ALWAYS_PRESENT).bloomFilterFpChance(1.0));
    }


    @Test
    public void testVerifyCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCounterCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF2);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCounterCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF2);

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).extendedVerification(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCorrectUncompressed()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF3);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCounterCorrectUncompressed()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF3);

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCorrectUncompressed()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF4);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().extendedVerification(true).invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCounterCorrectUncompressed()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF4);

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().extendedVerification(true).invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }


    @Test
    public void testVerifyIncorrectDigest() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF);

        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();


        try (RandomAccessReader file = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.DIGEST))))
        {
            Long correctChecksum = file.readLong();

            writeChecksum(++correctChecksum, sstable.descriptor.filenameFor(Component.DIGEST));
        }

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err) {}

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(false).build()))
        {
            verifier.verify();
            fail("Expected a RuntimeException to be thrown");
        }
        catch (RuntimeException err) {}
    }


    @Test
    public void testVerifyCorruptRowCorrectDigest() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);

        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // overwrite one row with garbage
        long row0Start = sstable.getPosition(PartitionPosition.ForKey.get(ByteBufferUtil.bytes("0"), cfs.getPartitioner()), SSTableReader.Operator.EQ).position;
        long row1Start = sstable.getPosition(PartitionPosition.ForKey.get(ByteBufferUtil.bytes("1"), cfs.getPartitioner()), SSTableReader.Operator.EQ).position;
        long startPosition = row0Start < row1Start ? row0Start : row1Start;
        long endPosition = row0Start < row1Start ? row1Start : row0Start;

        FileChannel file = new File(sstable.getFilename()).newReadWriteChannel();
        file.position(startPosition);
        file.write(ByteBufferUtil.bytes(StringUtils.repeat('z', 2)));
        file.close();
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFile(sstable.getFilename());

        // Update the Digest to have the right Checksum
        writeChecksum(simpleFullChecksum(sstable.getFilename()), sstable.descriptor.filenameFor(Component.DIGEST));

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            // First a simple verify checking digest, which should succeed
            try
            {
                verifier.verify();
            }
            catch (CorruptSSTableException err)
            {
                fail("Simple verify should have succeeded as digest matched");
            }
        }
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).extendedVerification(true).build()))
        {
            // Now try extended verify
            try
            {
                verifier.verify();

            }
            catch (CorruptSSTableException err)
            {
                return;
            }
            fail("Expected a CorruptSSTableException to be thrown");
        }
    }

    @Test
    public void testVerifyBrokenSSTableMetadata() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);
        cfs.truncateBlocking();
        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        String filenameToCorrupt = sstable.descriptor.filenameFor(Component.STATS);
        FileChannel file = new File(filenameToCorrupt).newReadWriteChannel();
        file.position(0);
        file.write(ByteBufferUtil.bytes(StringUtils.repeat('z', 2)));
        file.close();
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err)
        {}
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(false).build()))
        {
            verifier.verify();
            fail("Expected a RuntimeException to be thrown");
        }
        catch (CorruptSSTableException err) { fail("wrong exception thrown"); }
        catch (RuntimeException err)
        {}
    }

    @Test
    public void testVerifyMutateRepairStatus() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);
        cfs.truncateBlocking();
        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        // make the sstable repaired:
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, System.currentTimeMillis(), sstable.getPendingRepair(), sstable.isTransient());
        sstable.reloadSSTableMetadata();

        // break the sstable:
        Long correctChecksum;
        try (RandomAccessReader file = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.DIGEST))))
        {
            correctChecksum = file.readLong();
        }
        writeChecksum(++correctChecksum, sstable.descriptor.filenameFor(Component.DIGEST));
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().mutateRepairStatus(false).invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err)
        {}

        assertTrue(sstable.isRepaired());

        // now the repair status should be changed:
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().mutateRepairStatus(true).invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err)
        {}
        assertFalse(sstable.isRepaired());
    }

    @Test(expected = RuntimeException.class)
    public void testOutOfRangeTokens() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        fillCF(cfs, 100);
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk2), InetAddressAndPort.getByName("127.0.0.2"));

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().checkOwnsTokens(true).extendedVerification(true).build()))
        {
            verifier.verify();
        }
        finally
        {
            StorageService.instance.getTokenMetadata().clearUnsafe();
        }

    }

    @Test
    public void testMutateRepair() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1, sstable.getPendingRepair(), sstable.isTransient());
        sstable.reloadSSTableMetadata();
        cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
        assertTrue(sstable.isRepaired());
        cfs.forceMajorCompaction();

        sstable = cfs.getLiveSSTables().iterator().next();
        Long correctChecksum;
        try (RandomAccessReader file = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.DIGEST))))
        {
            correctChecksum = file.readLong();
        }
        writeChecksum(++correctChecksum, sstable.descriptor.filenameFor(Component.DIGEST));
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).mutateRepairStatus(true).build()))
        {
            verifier.verify();
            fail("should be corrupt");
        }
        catch (CorruptSSTableException e)
        {}
        assertFalse(sstable.isRepaired());
    }

    @Test
    public void testVerifyIndex() throws IOException
    {
        testBrokenComponentHelper(Component.PRIMARY_INDEX);
    }
    @Test
    public void testVerifyBf() throws IOException
    {
        testBrokenComponentHelper(Component.FILTER);
    }

    @Test
    public void testVerifyIndexSummary() throws IOException
    {
        testBrokenComponentHelper(Component.SUMMARY);
    }

    private void testBrokenComponentHelper(Component componentToBreak) throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().build()))
        {
            verifier.verify(); //still not corrupt, should pass
        }
        String filenameToCorrupt = sstable.descriptor.filenameFor(componentToBreak);
        try(FileChannel fileChannel = new File(filenameToCorrupt).newReadWriteChannel())
        {
            fileChannel.truncate(3);
        }

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("should throw exception");
        }
        catch(CorruptSSTableException e)
        {
            //expected
        }
    }

    @Test
    public void testQuick() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF);

        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();


        try (RandomAccessReader file = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.DIGEST))))
        {
            Long correctChecksum = file.readLong();

            writeChecksum(++correctChecksum, sstable.descriptor.filenameFor(Component.DIGEST));
        }

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err) {}

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).quick(true).build())) // with quick = true we don't verify the digest
        {
            verifier.verify();
        }

        try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a RuntimeException to be thrown");
        }
        catch (CorruptSSTableException err) {}
    }

    @Test
    public void testRangeOwnHelper()
    {
        List<Range<Token>> normalized = new ArrayList<>();
        normalized.add(r(Long.MIN_VALUE, Long.MIN_VALUE + 1));
        normalized.add(r(Long.MIN_VALUE + 5, Long.MIN_VALUE + 6));
        normalized.add(r(Long.MIN_VALUE + 10, Long.MIN_VALUE + 11));
        normalized.add(r(0,10));
        normalized.add(r(10,11));
        normalized.add(r(20,25));
        normalized.add(r(26,200));

        Verifier.RangeOwnHelper roh = new Verifier.RangeOwnHelper(normalized);

        roh.validate(dk(1));
        roh.validate(dk(10));
        roh.validate(dk(11));
        roh.validate(dk(21));
        roh.validate(dk(25));
        boolean gotException = false;
        try
        {
            roh.validate(dk(26));
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
    }

    @Test(expected = AssertionError.class)
    public void testRangeOwnHelperBadToken()
    {
        List<Range<Token>> normalized = new ArrayList<>();
        normalized.add(r(0,10));
        Verifier.RangeOwnHelper roh = new Verifier.RangeOwnHelper(normalized);
        roh.validate(dk(1));
        // call with smaller token to get exception
        roh.validate(dk(0));
    }


    @Test
    public void testRangeOwnHelperNormalize()
    {
        List<Range<Token>> normalized = Range.normalize(Collections.singletonList(r(0,0)));
        Verifier.RangeOwnHelper roh = new Verifier.RangeOwnHelper(normalized);
        roh.validate(dk(Long.MIN_VALUE));
        roh.validate(dk(0));
        roh.validate(dk(Long.MAX_VALUE));
    }

    @Test
    public void testRangeOwnHelperNormalizeWrap()
    {
        List<Range<Token>> normalized = Range.normalize(Collections.singletonList(r(Long.MAX_VALUE - 1000,Long.MIN_VALUE + 1000)));
        Verifier.RangeOwnHelper roh = new Verifier.RangeOwnHelper(normalized);
        roh.validate(dk(Long.MIN_VALUE));
        roh.validate(dk(Long.MAX_VALUE));
        boolean gotException = false;
        try
        {
            roh.validate(dk(26));
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
    }

    @Test
    public void testEmptyRanges()
    {
        new Verifier.RangeOwnHelper(Collections.emptyList()).validate(dk(1));
    }

    @Test
    public void testVerifyLocalPartitioner() throws UnknownHostException
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk2), InetAddressAndPort.getByName("127.0.0.2"));
        // write some bogus to a localpartitioner table
        Batch bogus = Batch.createLocal(nextTimeUUID(), 0, Collections.emptyList());
        BatchlogManager.store(bogus);
        ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("batches");
        Util.flush(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {

            try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().checkOwnsTokens(true).build()))
            {
                verifier.verify();
            }
        }
    }

    @Test
    public void testNoFilterFile()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(BF_ALWAYS_PRESENT);
        fillCF(cfs, 100);
        assertEquals(1.0, cfs.metadata().params.bloomFilterFpChance, 0.0);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            File f = new File(sstable.descriptor.filenameFor(Component.FILTER));
            assertFalse(f.exists());
            try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().build()))
            {
                verifier.verify();
            }
        }
    }



    private DecoratedKey dk(long l)
    {
        return new BufferDecoratedKey(t(l), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    private Range<Token> r(long s, long e)
    {
        return new Range<>(t(s), t(e));
    }

    private Token t(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }


    protected void fillCF(ColumnFamilyStore cfs, int partitionsPerSSTable)
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                         .newRow("c1").add("val", "1")
                         .newRow("c2").add("val", "2")
                         .apply();
        }

        Util.flush(cfs);
    }

    protected void fillCounterCF(ColumnFamilyStore cfs, int partitionsPerSSTable) throws WriteTimeoutException
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                         .newRow("c1").add("val", 100L)
                         .apply();
        }

        Util.flush(cfs);
    }

    protected long simpleFullChecksum(String filename) throws IOException
    {
        try (FileInputStreamPlus inputStream = new FileInputStreamPlus(filename))
        {
            CRC32 checksum = new CRC32();
            CheckedInputStream cinStream = new CheckedInputStream(inputStream, checksum);
            byte[] b = new byte[128];
            while (cinStream.read(b) >= 0) {
            }
            return cinStream.getChecksum().getValue();
        }
    }

    public static void writeChecksum(long checksum, String filePath)
    {
        File outFile = new File(filePath);
        BufferedWriter out = null;
        try
        {
            out = Files.newBufferedWriter(outFile.toPath(), Charsets.UTF_8);
            out.write(String.valueOf(checksum));
            out.flush();
            out.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, outFile);
        }
        finally
        {
            FileUtils.closeQuietly(out);
        }

    }

}
