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
package org.apache.cassandra.io.sstable.metadata;

import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.util.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetadataSerializerTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
<<<<<<< HEAD
=======
        format = DatabaseDescriptor.getSelectedSSTableFormat();
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
    }

    @Test
    public void testSerialization() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata();

        MetadataSerializer serializer = new MetadataSerializer();
<<<<<<< HEAD
        File statsFile = serialize(originalMetadata, serializer, BigFormat.latestVersion);

        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), SSTableFormat.Type.BIG);
=======
        File statsFile = serialize(originalMetadata, serializer, DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion());

        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), DatabaseDescriptor.getSelectedSSTableFormat());
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(originalMetadata.get(type), deserialized.get(type));
            }
        }
    }

    @Test
    public void testHistogramSterilization() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata();

        // Modify the histograms to overflow:
        StatsMetadata originalStats = (StatsMetadata) originalMetadata.get(MetadataType.STATS);
        originalStats.estimatedCellPerPartitionCount.add(Long.MAX_VALUE);
        originalStats.estimatedPartitionSize.add(Long.MAX_VALUE);
        assertTrue(originalStats.estimatedCellPerPartitionCount.isOverflowed());
        assertTrue(originalStats.estimatedPartitionSize.isOverflowed());

        // Serialize w/ overflowed histograms:
        MetadataSerializer serializer = new MetadataSerializer();
<<<<<<< HEAD
        File statsFile = serialize(originalMetadata, serializer, BigFormat.latestVersion);
        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), SSTableFormat.Type.BIG);
=======
        File statsFile = serialize(originalMetadata, serializer, format.getLatestVersion());
        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), format);
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f

        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            // Deserialie and verify that the two histograms have had their overflow buckets cleared:
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));
            StatsMetadata deserializedStats = (StatsMetadata) deserialized.get(MetadataType.STATS);
            assertFalse(deserializedStats.estimatedCellPerPartitionCount.isOverflowed());
            assertFalse(deserializedStats.estimatedPartitionSize.isOverflowed());
        }
    }

    public File serialize(Map<MetadataType, MetadataComponent> metadata, MetadataSerializer serializer, Version version)
    throws IOException
    {
        // Serialize to tmp file
        File statsFile = FileUtils.createTempFile(Component.STATS.name, null);
        try (DataOutputStreamPlus out = new FileOutputStreamPlus(statsFile))
        {
            serializer.serialize(metadata, out, version);
        }
        return statsFile;
    }

    public Map<MetadataType, MetadataComponent> constructMetadata()
    {
        CommitLogPosition club = new CommitLogPosition(11L, 12);
        CommitLogPosition cllb = new CommitLogPosition(9L, 12);

        TableMetadata cfm = SchemaLoader.standardCFMD("ks1", "cf1").build();
        MetadataCollector collector = new MetadataCollector(cfm.comparator)
                                      .commitLogIntervals(new IntervalSet<>(cllb, club));

        String partitioner = RandomPartitioner.class.getCanonicalName();
        double bfFpChance = 0.1;
        return collector.finalizeMetadata(partitioner, bfFpChance, 0, null, false, SerializationHeader.make(cfm, Collections.emptyList()));
    }

    private void testVersions(String... versions) throws Throwable
    {
        Throwable t = null;
        for (int oldIdx = 0; oldIdx < versions.length; oldIdx++)
        {
            for (int newIdx = oldIdx; newIdx < versions.length; newIdx++)
            {
                try
                {
                    testOldReadsNew(versions[oldIdx], versions[newIdx]);
                }
                catch (Exception | AssertionError e)
                {
                    t = Throwables.merge(t, new AssertionError("Failed to test " + versions[oldIdx] + " -> " + versions[newIdx], e));
                }
            }
        }
        if (t != null)
        {
            throw t;
        }
    }

    @Test
    public void testMVersions() throws Throwable
    {
<<<<<<< HEAD
        testVersions("ma", "mb", "mc", "md", "me");
    }
=======
        Map<Character, List<String>> supportedVersions = new LinkedHashMap<>();
        for (char major = 'a'; major <= 'z'; major++){
            for (char minor = 'a'; minor <= 'z'; minor++){
                Version version = format.getVersion(String.format("%s%s", major, minor));
                if (version.isCompatible())
                    supportedVersions.computeIfAbsent(major, ignored -> new ArrayList<>()).add(version.version);
            }
        }
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f

    @Test
    public void testNVersions() throws Throwable
    {
        testVersions("na", "nb");
    }

    public void testOldReadsNew(String oldV, String newV) throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata();

        MetadataSerializer serializer = new MetadataSerializer();
        // Write metadata in two minor formats.
        File statsFileLb = serialize(originalMetadata, serializer, BigFormat.instance.getVersion(newV));
        File statsFileLa = serialize(originalMetadata, serializer, BigFormat.instance.getVersion(oldV));
        // Reading both as earlier version should yield identical results.
<<<<<<< HEAD
        SSTableFormat.Type stype = SSTableFormat.Type.current();
        Descriptor desc = new Descriptor(stype.info.getVersion(oldV), statsFileLb.parent(), "", "", new SequenceBasedSSTableId(0), stype);
=======
        Descriptor desc = new Descriptor(format.getVersion(oldV), statsFileLb.parent(), "", "", new SequenceBasedSSTableId(0));
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
        try (RandomAccessReader inLb = RandomAccessReader.open(statsFileLb);
             RandomAccessReader inLa = RandomAccessReader.open(statsFileLa))
        {
            Map<MetadataType, MetadataComponent> deserializedLb = serializer.deserialize(desc, inLb, EnumSet.allOf(MetadataType.class));
            Map<MetadataType, MetadataComponent> deserializedLa = serializer.deserialize(desc, inLa, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(deserializedLa.get(type), deserializedLb.get(type));

                if (MetadataType.STATS != type)
                    assertEquals(originalMetadata.get(type), deserializedLb.get(type));
            }
        }
    }

    @Test
    public void pendingRepairCompatibility()
    {
<<<<<<< HEAD
        Arrays.asList("ma", "mb", "mc", "md", "me").forEach(v -> assertFalse(BigFormat.instance.getVersion(v).hasPendingRepair()));
        Arrays.asList("na", "nb").forEach(v -> assertTrue(BigFormat.instance.getVersion(v).hasPendingRepair()));
=======
        if (BigFormat.is(format))
        {
            Arrays.asList("ma", "mb", "mc", "md", "me").forEach(v -> assertFalse(format.getVersion(v).hasPendingRepair()));
            Arrays.asList("na", "nb", "nc").forEach(v -> assertTrue(format.getVersion(v).hasPendingRepair()));
        }
        else
        {
            throw Util.testMustBeImplementedForSSTableFormat();
        }
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
    }

    @Test
    public void originatingHostCompatibility()
    {
<<<<<<< HEAD
        Arrays.asList("ma", "mb", "mc", "md", "na").forEach(v -> assertFalse(BigFormat.instance.getVersion(v).hasOriginatingHostId()));
        Arrays.asList("me", "nb").forEach(v -> assertTrue(BigFormat.instance.getVersion(v).hasOriginatingHostId()));
=======
        if (BigFormat.is(format))
        {
            Arrays.asList("ma", "mb", "mc", "md", "na").forEach(v -> assertFalse(format.getVersion(v).hasOriginatingHostId()));
            Arrays.asList("me", "nb").forEach(v -> assertTrue(format.getVersion(v).hasOriginatingHostId()));
        }
        else
        {
            throw Util.testMustBeImplementedForSSTableFormat();
        }
    }

    @Test
    public void improvedMinMaxCompatibility()
    {
        if (BigFormat.is(format))
        {
            Arrays.asList("ma", "mb", "mc", "md", "me", "na", "nb").forEach(v -> assertFalse(BigFormat.getInstance().getVersion(v).hasImprovedMinMax()));
            Arrays.asList("nc", "oa").forEach(v -> assertTrue(BigFormat.getInstance().getVersion(v).hasImprovedMinMax()));
        }
        else
        {
            throw Util.testMustBeImplementedForSSTableFormat();
        }
    }

    @Test
    public void legacyMinMaxCompatiblity()
    {
        if (BigFormat.is(format))
        {
            Arrays.asList("oa").forEach(v -> assertFalse(BigFormat.getInstance().getVersion(v).hasLegacyMinMax()));
            Arrays.asList("ma", "mb", "mc", "md", "me", "na", "nb", "nc").forEach(v -> assertTrue(BigFormat.getInstance().getVersion(v).hasLegacyMinMax()));
        }
        else
        {
            throw Util.testMustBeImplementedForSSTableFormat();
        }
    }

    @Test
    public void partitionLevelDeletionPresenceMarkerCompatibility()
    {
        if (BigFormat.is(format))
        {
            Arrays.asList("ma", "mb", "mc", "md", "me", "na", "nb").forEach(v -> assertFalse(BigFormat.getInstance().getVersion(v).hasPartitionLevelDeletionsPresenceMarker()));
            Arrays.asList("nc", "oa").forEach(v -> assertTrue(BigFormat.getInstance().getVersion(v).hasPartitionLevelDeletionsPresenceMarker()));
        }
        else
        {
            throw Util.testMustBeImplementedForSSTableFormat();
        }
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
    }
}
