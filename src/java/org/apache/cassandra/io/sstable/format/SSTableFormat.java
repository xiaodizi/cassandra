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
package org.apache.cassandra.io.sstable.format;

<<<<<<< HEAD
<<<<<<< HEAD
import com.google.common.base.CharMatcher;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
=======
=======
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.MetricsProviders;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f

/**
 * Provides the accessors to data on disk.
 */
public interface SSTableFormat
{
<<<<<<< HEAD
<<<<<<< HEAD
    static boolean enableSSTableDevelopmentTestMode = Boolean.getBoolean("cassandra.test.sstableformatdevelopment");

=======
=======
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
    String name();
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f

    Version getLatestVersion();
    Version getVersion(String version);

    SSTableWriter.Factory getWriterFactory();
    SSTableReader.Factory getReaderFactory();

    RowIndexEntry.IndexSerializer<?> getIndexSerializer(TableMetadata metadata, Version version, SerializationHeader header);

<<<<<<< HEAD
    public static enum Type
    {
        //The original sstable format
        BIG("big", BigFormat.instance);

        public final SSTableFormat info;
        public final String name;

        public static Type current()
        {
            return BIG;
        }

        private Type(String name, SSTableFormat info)
        {
            //Since format comes right after generation
            //we disallow formats with numeric names
            assert !CharMatcher.digit().matchesAllOf(name);

            this.name = name;
            this.info = info;
        }

        public static Type validate(String name)
        {
            for (Type valid : Type.values())
            {
                if (valid.name.equalsIgnoreCase(name))
                    return valid;
            }

            throw new IllegalArgumentException("No Type constant " + name);
        }
=======
    /**
     * All the components that the writter can produce when saving an sstable, as well as all the components
     * that the reader can read.
     */
    Set<Component> allComponents();

    Set<Component> streamingComponents();

    Set<Component> primaryComponents();

    /**
     * Returns components required by offline compaction tasks - like splitting sstables.
     */
    Set<Component> batchComponents();

    /**
     * Returns the components which should be selected for upload by the sstable loader.
     */
    Set<Component> uploadComponents();

    /**
     * Returns a set of the components that can be changed after an sstable was written.
     */
    Set<Component> mutableComponents();

    /**
     * Returns a set of components that can be automatically generated when loading sstable and thus are not mandatory.
     */
    Set<Component> generatedOnLoadComponents();

    KeyCacheValueSerializer<R, ?> getKeyCacheValueSerializer();

    /**
     * Returns a new scrubber for an sstable. Note that the transaction must contain only one reader
     * and the reader must match the provided cfs.
     */
    IScrubber getScrubber(ColumnFamilyStore cfs,
                          LifecycleTransaction transaction,
                          OutputHandler outputHandler,
                          IScrubber.Options options);

    R cast(SSTableReader sstr);

    W cast(SSTableWriter sstw);

    MetricsProviders getFormatSpecificMetricsProviders();

    void deleteOrphanedComponents(Descriptor descriptor, Set<Component> components);

    /**
     * Deletes the existing components of the sstables represented by the provided descriptor.
     * The method is also responsible for cleaning up the in-memory resources occupied by the stuff related to that
     * sstables, such as row key cache entries.
     */
    void delete(Descriptor descriptor);

    interface SSTableReaderFactory<R extends SSTableReader, B extends SSTableReader.Builder<R, B>>
    {
        /**
         * A simple builder which creates an instnace of {@link SSTableReader} with the provided parameters.
         * It expects that all the required resources to be opened/loaded externally by the caller.
         * <p>
         * The builder is expected to perform basic validation of the provided parameters.
         */
        SSTableReader.Builder<R, B> builder(Descriptor descriptor);

        /**
         * A builder which opens/loads all the required resources upon execution of
         * {@link SSTableReaderLoadingBuilder#build(SSTable.Owner, boolean, boolean)} and passed them to the created
         * reader instance. If the creation of {@link SSTableReader} fails, no resources should be left opened.
         * <p>
         * The builder is expected to perform basic validation of the provided parameters.
         */
        SSTableReaderLoadingBuilder<R, B> loadingBuilder(Descriptor descriptor, TableMetadataRef tableMetadataRef, Set<Component> components);

        /**
         * Retrieves a key range for the given sstable at the lowest cost - that is, without opening all sstables files
         * if possible.
         */
        Pair<DecoratedKey, DecoratedKey> readKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException;

        Class<R> getReaderClass();
    }

    interface SSTableWriterFactory<W extends SSTableWriter, B extends SSTableWriter.Builder<W, B>>
    {
        /**
         * Returns a new builder which can create instance of {@link SSTableWriter} with the provided parameters.
         * Similarly to the loading builder, it should open the required resources when
         * the {@link SSTableWriter.Builder#build(LifecycleNewTracker, SSTable.Owner)} method is called.
         * It should not let the caller passing any closeable resources directly, that is, via setters.
         * If building fails, all the opened resources should be released.
         */
        B builder(Descriptor descriptor);

        /**
         * Tries to estimate the size of all the sstable files from the provided parameters.
         */
        long estimateSize(SSTableWriter.SSTableSizeParameters parameters);
    }

    class Components
    {
        public static class Types
        {
            // the base data for an sstable: the remaining components can be regenerated
            // based on the data component
            public static final Component.Type DATA = Component.Type.createSingleton("DATA", "Data.db", null);
            // file to hold information about uncompressed data length, chunk offsets etc.
            public static final Component.Type COMPRESSION_INFO = Component.Type.createSingleton("COMPRESSION_INFO", "CompressionInfo.db", null);
            // statistical metadata about the content of the sstable
            public static final Component.Type STATS = Component.Type.createSingleton("STATS", "Statistics.db", null);
            // serialized bloom filter for the row keys in the sstable
            public static final Component.Type FILTER = Component.Type.createSingleton("FILTER", "Filter.db", null);
            // holds CRC32 checksum of the data file
            public static final Component.Type DIGEST = Component.Type.createSingleton("DIGEST", "Digest.crc32", null);
            // holds the CRC32 for chunks in an uncompressed file.
            public static final Component.Type CRC = Component.Type.createSingleton("CRC", "CRC.db", null);
            // table of contents, stores the list of all components for the sstable
            public static final Component.Type TOC = Component.Type.createSingleton("TOC", "TOC.txt", null);
            // built-in secondary index (may exist multiple per sstable)
            public static final Component.Type SECONDARY_INDEX = Component.Type.create("SECONDARY_INDEX", "SI_.*.db", null);
            // custom component, used by e.g. custom compaction strategy
            public static final Component.Type CUSTOM = Component.Type.create("CUSTOM", null, null);
        }

        // singleton components for types that don't need ids
        public final static Component DATA = Types.DATA.getSingleton();
        public final static Component COMPRESSION_INFO = Types.COMPRESSION_INFO.getSingleton();
        public final static Component STATS = Types.STATS.getSingleton();
        public final static Component FILTER = Types.FILTER.getSingleton();
        public final static Component DIGEST = Types.DIGEST.getSingleton();
        public final static Component CRC = Types.CRC.getSingleton();
        public final static Component TOC = Types.TOC.getSingleton();
    }

    interface KeyCacheValueSerializer<R extends SSTableReader, T extends AbstractRowIndexEntry>
    {
        void skip(DataInputPlus input) throws IOException;

        T deserialize(R reader, DataInputPlus input) throws IOException;

        void serialize(T entry, DataOutputPlus output) throws IOException;
    }

    interface Factory
    {
        /**
         * Returns a name of the format. Format name must not be empty, must be unique and must consist only of lowercase letters.
         */
        String name();

        /**
         * Returns an instance of the sstable format configured with the provided options.
         * <p/>
         * The method is expected to validate the options, and throw
         * {@link org.apache.cassandra.exceptions.ConfigurationException} if the validation fails.
         *
         * @param options    overrides for the default options, can be empty, cannot be null
         */
        SSTableFormat<?, ?> getInstance(@Nonnull Map<String, String> options);
<<<<<<< HEAD
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
=======
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
    }
}
