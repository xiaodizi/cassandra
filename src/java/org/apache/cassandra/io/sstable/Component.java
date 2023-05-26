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
package org.apache.cassandra.io.sstable;

import java.util.EnumSet;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

/**
 * SSTables are made up of multiple components in separate files. Components are
 * identified by a type and an id, but required unique components (such as the Data
 * and Index files) may have implicit ids assigned to them.
 */
public class Component
{
    public static final char separator = '-';

    final static EnumSet<Type> TYPES = EnumSet.allOf(Type.class);

    /**
     * WARNING: Be careful while changing the names or string representation of the enum
     * members. Streaming code depends on the names during streaming (Ref: CASSANDRA-14556).
     */
    public enum Type
    {
        // the base data for an sstable: the remaining components can be regenerated
        // based on the data component
        DATA("Data.db"),
        // index of the row keys with pointers to their positions in the data file
        PRIMARY_INDEX("Index.db"),
        // serialized bloom filter for the row keys in the sstable
        FILTER("Filter.db"),
        // file to hold information about uncompressed data length, chunk offsets etc.
        COMPRESSION_INFO("CompressionInfo.db"),
        // statistical metadata about the content of the sstable
        STATS("Statistics.db"),
        // holds CRC32 checksum of the data file
        DIGEST("Digest.crc32"),
        // holds the CRC32 for chunks in an a uncompressed file.
        CRC("CRC.db"),
        // holds SSTable Index Summary (sampling of Index component)
        SUMMARY("Summary.db"),
        // table of contents, stores the list of all components for the sstable
        TOC("TOC.txt"),
        // built-in secondary index (may be multiple per sstable)
        SECONDARY_INDEX("SI_.*.db"),
        // custom component, used by e.g. custom compaction strategy
        CUSTOM(null);

        final String repr;

        Type(String repr)
        {
            this.repr = repr;
        }

        @VisibleForTesting
<<<<<<< HEAD
        public static Type fromRepresentation(String repr)
=======
        public static Type fromRepresentation(String repr, SSTableFormat<?, ?> format)
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
        {
            for (Type type : TYPES)
            {
<<<<<<< HEAD
                if (type.repr != null && Pattern.matches(type.repr, repr))
                    return type;
            }
            return CUSTOM;
=======
                if (type.repr != null && Pattern.matches(type.repr, repr) && type.formatClass.isAssignableFrom(format.getClass()))
                    return type;
            }
            return Types.CUSTOM;
        }

        public static Component createComponent(String repr, SSTableFormat<?, ?> format)
        {
            Type type = fromRepresentation(repr, format);
            if (type.singleton != null)
                return type.singleton;
            else
                return new Component(type, repr);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Type type = (Type) o;
            return id == type.id;
        }

        @Override
        public int hashCode()
        {
            return id;
        }

        @Override
        public String toString()
        {
            return name;
        }

        public Component getSingleton()
        {
            return Objects.requireNonNull(singleton);
        }

        public Component createComponent(String repr)
        {
            Preconditions.checkArgument(singleton == null);
            return new Component(this, repr);
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
        }
    }

    // singleton components for types that don't need ids
    public final static Component DATA = new Component(Type.DATA);
    public final static Component PRIMARY_INDEX = new Component(Type.PRIMARY_INDEX);
    public final static Component FILTER = new Component(Type.FILTER);
    public final static Component COMPRESSION_INFO = new Component(Type.COMPRESSION_INFO);
    public final static Component STATS = new Component(Type.STATS);
    public final static Component DIGEST = new Component(Type.DIGEST);
    public final static Component CRC = new Component(Type.CRC);
    public final static Component SUMMARY = new Component(Type.SUMMARY);
    public final static Component TOC = new Component(Type.TOC);

    public final Type type;
    public final String name;
    public final int hashCode;

    public Component(Type type)
    {
        this(type, type.repr);
        assert type != Type.CUSTOM;
    }

    public Component(Type type, String name)
    {
        assert name != null : "Component name cannot be null";
        this.type = type;
        this.name = name;
        this.hashCode = Objects.hashCode(type, name);
    }

    /**
     * @return The unique (within an sstable) name for this component.
     */
    public String name()
    {
        return name;
    }

    /**
     * Parse the component part of a sstable filename into a {@code Component} object.
     *
     * @param name a string representing a sstable component.
     * @return the component corresponding to {@code name}. Note that this always return a component as an unrecognized
     * name is parsed into a CUSTOM component.
     */
<<<<<<< HEAD
    public static Component parse(String name)
    {
        Type type = Type.fromRepresentation(name);

        // Build (or retrieve singleton for) the component object
        switch (type)
        {
            case DATA:             return Component.DATA;
            case PRIMARY_INDEX:    return Component.PRIMARY_INDEX;
            case FILTER:           return Component.FILTER;
            case COMPRESSION_INFO: return Component.COMPRESSION_INFO;
            case STATS:            return Component.STATS;
            case DIGEST:           return Component.DIGEST;
            case CRC:              return Component.CRC;
            case SUMMARY:          return Component.SUMMARY;
            case TOC:              return Component.TOC;
            case SECONDARY_INDEX:  return new Component(Type.SECONDARY_INDEX, name);
            case CUSTOM:           return new Component(Type.CUSTOM, name);
            default:               throw new AssertionError();
        }
=======
    public static Component parse(String name, SSTableFormat<?, ?> format)
    {
        return Type.createComponent(name, format);
    }

    public static Iterable<Component> getSingletonsFor(SSTableFormat<?, ?> format)
    {
        return Iterables.transform(Iterables.filter(Type.all, t -> t.singleton != null && t.formatClass.isAssignableFrom(format.getClass())), t -> t.singleton);
    }

    public static Iterable<Component> getSingletonsFor(Class<? extends SSTableFormat<?, ?>> formatClass)
    {
        return Iterables.transform(Iterables.filter(Type.all, t -> t.singleton != null && t.formatClass.isAssignableFrom(formatClass)), t -> t.singleton);
    }

    public boolean isValidFor(Descriptor descriptor)
    {
        return type.formatClass.isAssignableFrom(descriptor.version.format.getClass());
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
    }

    @Override
    public String toString()
    {
        return this.name();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Component))
            return false;
        Component that = (Component)o;
        return this.type == that.type && this.name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
