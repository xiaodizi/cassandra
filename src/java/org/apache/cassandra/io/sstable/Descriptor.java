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

import java.util.*;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.IMetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Pair;

<<<<<<< HEAD
=======
import static com.google.common.base.Preconditions.checkNotNull;
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
import static org.apache.cassandra.io.sstable.Component.separator;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * A SSTable is described by the keyspace and column family it contains data
 * for, an id (generation - where higher generations contain more recent data) and
 * an alphabetic version string.
 *
 * A descriptor can be marked as temporary, which influences generated filenames.
 */
public class Descriptor
{
    private final static String LEGACY_TMP_REGEX_STR = "^((.*)\\-(.*)\\-)?tmp(link)?\\-((?:l|k).)\\-(\\d)*\\-(.*)$";
    private final static Pattern LEGACY_TMP_REGEX = Pattern.compile(LEGACY_TMP_REGEX_STR);

    public static String TMP_EXT = ".tmp";

    public static final char FILENAME_SEPARATOR = '-';

    private static final Splitter filenameSplitter = Splitter.on(FILENAME_SEPARATOR);

    /** canonicalized path to the directory where SSTable resides */
    public final File directory;
    /** version has the following format: <code>[a-z]+</code> */
    public final Version version;
    public final String ksname;
    public final String cfname;
    public final SSTableId id;
    private final int hashCode;

    /**
     * A descriptor that assumes CURRENT_VERSION.
     */
    @VisibleForTesting
    public Descriptor(File directory, String ksname, String cfname, SSTableId id)
    {
        this(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion(), directory, ksname, cfname, id);
    }

    /**
     * Constructor for sstable writers only.
     */
    public Descriptor(File directory, String ksname, String cfname, SSTableId id, SSTableFormat<?, ?> format)
    {
        this(format.getLatestVersion(), directory, ksname, cfname, id);
    }

    @VisibleForTesting
    public Descriptor(String version, File directory, String ksname, String cfname, SSTableId id, SSTableFormat<?, ?> format)
    {
        this(format.getVersion(version), directory, ksname, cfname, id);
    }

    public Descriptor(Version version, File directory, String ksname, String cfname, SSTableId id)
    {
<<<<<<< HEAD
        assert version != null && directory != null && ksname != null && cfname != null && formatType.info.getLatestVersion().getClass().equals(version.getClass());
=======
        checkNotNull(version);
        checkNotNull(directory);
        checkNotNull(ksname);
        checkNotNull(cfname);

>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
        this.version = version;
        this.directory = directory.toCanonical();
        this.ksname = ksname;
        this.cfname = cfname;
        this.id = id;

        // directory is unnecessary for hashCode, and for simulator consistency we do not include it
        hashCode = Objects.hashCode(version, id, ksname, cfname);
    }

    public String tmpFilenameFor(Component component)
    {
        return filenameFor(component) + TMP_EXT;
    }

    /**
     * @return a unique temporary file name for given component during entire-sstable-streaming.
     */
    public String tmpFilenameForStreaming(Component component)
    {
        // Use UUID to handle concurrent streamings on the same sstable.
        // TMP_EXT allows temp file to be removed by {@link ColumnFamilyStore#scrubDataDirectories}
        return String.format("%s.%s%s", filenameFor(component), nextTimeUUID(), TMP_EXT);
    }

    public String filenameFor(Component component)
    {
        return baseFilename() + separator + component.name();
    }

    public File fileFor(Component component)
    {
        return new File(filenameFor(component));
    }

    public String baseFilename()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(directory).append(File.pathSeparator());
        appendFileName(buff);
        return buff.toString();
    }

    private void appendFileName(StringBuilder buff)
    {
        buff.append(version).append(separator);
        buff.append(id.toString());
        buff.append(separator).append(version.format.name());
    }

    public String relativeFilenameFor(Component component)
    {
        final StringBuilder buff = new StringBuilder();
        if (Directories.isSecondaryIndexFolder(directory))
        {
            buff.append(directory.name()).append(File.pathSeparator());
        }

        appendFileName(buff);
        buff.append(separator).append(component.name());
        return buff.toString();
    }

    public SSTableFormat getFormat()
    {
        return version.format;
    }

    /** Return any temporary files found in the directory */
    public List<File> getTemporaryFiles()
    {
        File[] tmpFiles = directory.tryList((dir, name) ->
                                              name.endsWith(Descriptor.TMP_EXT));

        List<File> ret = new ArrayList<>(tmpFiles.length);
        for (File tmpFile : tmpFiles)
            ret.add(tmpFile);

        return ret;
    }

    public static boolean isValidFile(File file)
    {
        String filename = file.name();
        return filename.endsWith(".db") && !LEGACY_TMP_REGEX.matcher(filename).matches();
    }

    /**
     * Parse a sstable filename into a Descriptor.
     * <p>
     * This is a shortcut for {@code fromFilename(new File(filename))}.
     *
     * @param filename the filename to a sstable component.
     * @return the descriptor for the parsed file.
     *
     * @throws IllegalArgumentException if the provided {@code file} does point to a valid sstable filename. This could
     * mean either that the filename doesn't look like a sstable file, or that it is for an old and unsupported
     * versions.
     */
    public static Descriptor fromFilename(String filename)
    {
        return fromFilename(new File(filename));
    }

    /**
     * Parse a sstable filename into a Descriptor.
     * <p>
     * SSTables files are all located within subdirectories of the form {@code <keyspace>/<table>/}. Normal sstables are
     * are directly within that subdirectory structure while 2ndary index, backups and snapshot are each inside an
     * additional subdirectory. The file themselves have the form:
     *   {@code <version>-<gen>-<format>-<component>}.
     * <p>
     * Note that this method will only sucessfully parse sstable files of supported versions.
     *
     * @param file the {@code File} object for the filename to parse.
     * @return the descriptor for the parsed file.
     *
     * @throws IllegalArgumentException if the provided {@code file} does point to a valid sstable filename. This could
     * mean either that the filename doesn't look like a sstable file, or that it is for an old and unsupported
     * versions.
     */
    public static Descriptor fromFilename(File file)
    {
        return fromFilenameWithComponent(file).left;
    }

    public static Component componentFromFile(File file)
    {
        String name = file.name();
        List<String> tokens = filenameTokens(name);

        return Component.parse(tokens.get(3), formatFromName(name, tokens));
    }

    private static SSTableFormat<?, ?> formatFromName(String fileName, List<String> tokens)
    {
        String formatString = tokens.get(2);
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSSTableFormats().get(formatString);
        if (format == null)
            throw invalidSSTable(fileName, "unknown 'format' part (%s)", formatString);
        return format;
    }

    /**
     * Parse a sstable filename, extracting both the {@code Descriptor} and {@code Component} part.
     *
     * @param file the {@code File} object for the filename to parse.
     * @return a pair of the descriptor and component corresponding to the provided {@code file}.
     *
     * @throws IllegalArgumentException if the provided {@code file} does point to a valid sstable filename. This could
     * mean either that the filename doesn't look like a sstable file, or that it is for an old and unsupported
     * versions.
     */
    public static Pair<Descriptor, Component> fromFilenameWithComponent(File file)
    {
        // We need to extract the keyspace and table names from the parent directories, so make sure we deal with the
        // absolute path.
        if (!file.isAbsolute())
            file = file.toAbsolute();

<<<<<<< HEAD
        String name = file.name();
=======
        SSTableInfo info = validateAndExtractInfo(file);
        String name = file.name();

        String keyspaceName = "";
        String tableName = "";

        Matcher sstableDirMatcher = SSTABLE_DIR_PATTERN.matcher(file.toString());

        // Use pre-2.1 SSTable format if current one does not match it
        if (!sstableDirMatcher.find(0))
        {
            sstableDirMatcher = LEGACY_SSTABLE_DIR_PATTERN.matcher(file.toString());
        }

        if (sstableDirMatcher.find(0))
        {
            keyspaceName = sstableDirMatcher.group("keyspace");
            tableName = sstableDirMatcher.group("tableName");
            String indexName = sstableDirMatcher.group("indexName");
            if (indexName != null)
            {
                tableName = String.format("%s.%s", tableName, indexName);
            }
        }
        else if (validateDirs)
        {
            logger.debug("Could not extract keyspace/table info from sstable directory {}", file.toString());
            throw invalidSSTable(name, String.format("cannot extract keyspace and table name from %s; make sure the sstable is in the proper sub-directories", file));
        }

        return Pair.create(new Descriptor(info.version, parentOf(name, file), keyspaceName, tableName, info.id), info.component);
    }

    /**
     * Parse a sstable filename, extracting both the {@code Descriptor} and {@code Component} part.
     *
     * @param file     the {@code File} object for the filename to parse.
     * @param keyspace The keyspace name of the file. If <code>null</code>, then the keyspace name will be extracted
     *                 from the directory path.
     * @param table    The table name of the file. If <code>null</code>, then the table name will be extracted from the
     *                 directory path.
     * @return a pair of the descriptor and component corresponding to the provided {@code file}.
     * @throws IllegalArgumentException if the provided {@code file} does point to a valid sstable filename. This could
     *                                  mean either that the filename doesn't look like a sstable file, or that it is for an old and unsupported
     *                                  versions.
     */
    public static Pair<Descriptor, Component> fromFileWithComponent(File file, String keyspace, String table)
    {
        if (null == keyspace || null == table)
        {
            return fromFileWithComponent(file);
        }

        SSTableInfo info = validateAndExtractInfo(file);
        return Pair.create(new Descriptor(info.version, parentOf(file.name(), file), keyspace, table, info.id), info.component);
    }

    private static List<String> filenameTokens(String name)
    {
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
        List<String> tokens = filenameSplitter.splitToList(name);
        int size = tokens.size();

        if (size != 4)
        {
            // This is an invalid sstable file for this version. But to provide a more helpful error message, we detect
            // old format sstable, which had the format:
            //   <keyspace>-<table>-(tmp-)?<version>-<gen>-<component>
            // Note that we assume it's an old format sstable if it has the right number of tokens: this is not perfect
            // but we're just trying to be helpful, not perfect.
            if (size == 5 || size == 6)
                throw new IllegalArgumentException(String.format("%s is of version %s which is now unsupported and cannot be read.",
                                                                 name,
                                                                 tokens.get(size - 3)));
            throw new IllegalArgumentException(String.format("Invalid sstable file %s: the name doesn't look like a supported sstable file name", name));
        }
        return tokens;
    }

    private static SSTableInfo validateAndExtractInfo(File file)
    {
        String name = file.name();
        List<String> tokens = filenameTokens(name);

        String versionString = tokens.get(0);
        if (!Version.validate(versionString))
            throw invalidSSTable(name, "invalid version %s", versionString);

        SSTableId id;
        try
        {
            id = SSTableIdFactory.instance.fromString(tokens.get(1));
        }
        catch (RuntimeException e)
        {
            throw invalidSSTable(name, "the 'id' part (%s) of the name doesn't parse as a valid unique identifier", tokens.get(1));
        }

<<<<<<< HEAD
        String formatString = tokens.get(2);
        SSTableFormat.Type format;
        try
        {
            format = SSTableFormat.Type.validate(formatString);
        }
        catch (RuntimeException e)
        {
            throw invalidSSTable(name, "unknown 'format' part (%s)", formatString);
        }

        Component component = Component.parse(tokens.get(3));
=======
        SSTableFormat<?, ?> format = formatFromName(name, tokens);
        Component component = Component.parse(tokens.get(3), format);
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f

        Version version = format.getVersion(versionString);
        if (!version.isCompatible())
            throw invalidSSTable(name, "incompatible sstable version (%s); you should have run upgradesstables before upgrading", versionString);

<<<<<<< HEAD
        File directory = parentOf(name, file);
        File tableDir = directory;

        // Check if it's a 2ndary index directory (not that it doesn't exclude it to be also a backup or snapshot)
        String indexName = "";
        if (Directories.isSecondaryIndexFolder(tableDir))
        {
            indexName = tableDir.name();
            tableDir = parentOf(name, tableDir);
=======
        return new SSTableInfo(version, id, component);
    }

    private static class SSTableInfo
    {
        final Version version;
        final SSTableId id;
        final Component component;

        SSTableInfo(Version version, SSTableId id, Component component)
        {
            this.version = version;
            this.id = id;
            this.component = component;
>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
        }

        // Then it can be a backup or a snapshot
        if (tableDir.name().equals(Directories.BACKUPS_SUBDIR) && tableDir.parent().name().contains("-"))
            tableDir = tableDir.parent();
        else
        {
            File keyspaceOrSnapshotDir = parentOf(name, tableDir);
            if (keyspaceOrSnapshotDir.name().equals(Directories.SNAPSHOT_SUBDIR)
                && parentOf(name, keyspaceOrSnapshotDir).name().contains("-"))
                tableDir = parentOf(name, keyspaceOrSnapshotDir);
        }

        String table = tableDir.name().split("-")[0] + indexName;
        String keyspace = parentOf(name, tableDir).name();

        return Pair.create(new Descriptor(version, directory, keyspace, table, id, format), component);
    }

    private static File parentOf(String name, File file)
    {
        File parent = file.parent();
        if (parent == null)
            throw invalidSSTable(name, "cannot extract keyspace and table name; make sure the sstable is in the proper sub-directories");
        return parent;
    }

    private static IllegalArgumentException invalidSSTable(String name, String msgFormat, Object... parameters)
    {
        throw new IllegalArgumentException(String.format("Invalid sstable file " + name + ": " + msgFormat, parameters));
    }

    public IMetadataSerializer getMetadataSerializer()
    {
        return new MetadataSerializer();
    }

    /**
     * @return true if the current Cassandra version can read the given sstable version
     */
    public boolean isCompatible()
    {
        return version.isCompatible();
    }

<<<<<<< HEAD
=======
    public Set<Component> discoverComponents()
    {
        Set<Component> components = Sets.newHashSetWithExpectedSize(Component.Type.all.size());
        for (Component component : Component.getSingletonsFor(version.format))
        {
            if (fileFor(component).exists())
                components.add(component);
        }
        return components;
    }

>>>>>>> b0aa44b27da97b37345ee6fafbee16d66f3b384f
    @Override
    public String toString()
    {
        return baseFilename();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Descriptor))
            return false;
        Descriptor that = (Descriptor)o;
        return that.directory.equals(this.directory)
                       && that.id.equals(this.id)
                       && that.ksname.equals(this.ksname)
                       && that.cfname.equals(this.cfname)
                       && that.version.equals(this.version);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
