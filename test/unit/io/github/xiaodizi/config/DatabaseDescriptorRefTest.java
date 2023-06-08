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

package io.github.xiaodizi.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import org.junit.Test;

import io.github.xiaodizi.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Verifies that {@link DatabaseDescriptor#clientInitialization()} } and a couple of <i>apply</i> methods
 * do not somehow lazily initialize any unwanted part of Cassandra like schema, commit log or start
 * unexpected threads.
 *
 * {@link DatabaseDescriptor#toolInitialization()} is tested via unit tests extending
 * {@link io.github.xiaodizi.tools.OfflineToolUtils}.
 */
public class DatabaseDescriptorRefTest
{
    static final String[] validClasses = {
    "io.github.xiaodizi.audit.AuditLogOptions",
    "io.github.xiaodizi.audit.BinAuditLogger",
    "io.github.xiaodizi.audit.BinLogAuditLogger",
    "io.github.xiaodizi.audit.IAuditLogger",
    "io.github.xiaodizi.auth.AllowAllInternodeAuthenticator",
    "io.github.xiaodizi.auth.AuthCache$BulkLoader",
    "io.github.xiaodizi.auth.Cacheable",
    "io.github.xiaodizi.auth.IInternodeAuthenticator",
    "io.github.xiaodizi.auth.IAuthenticator",
    "io.github.xiaodizi.auth.IAuthorizer",
    "io.github.xiaodizi.auth.IRoleManager",
    "io.github.xiaodizi.auth.INetworkAuthorizer",
    "io.github.xiaodizi.config.DatabaseDescriptor",
    "io.github.xiaodizi.config.CassandraRelevantProperties",
    "io.github.xiaodizi.config.CassandraRelevantProperties$PropertyConverter",
    "io.github.xiaodizi.config.ConfigurationLoader",
    "io.github.xiaodizi.config.Config",
    "io.github.xiaodizi.config.Config$1",
    "io.github.xiaodizi.config.Config$CommitLogSync",
    "io.github.xiaodizi.config.Config$CommitFailurePolicy",
    "io.github.xiaodizi.config.Config$DiskAccessMode",
    "io.github.xiaodizi.config.Config$DiskFailurePolicy",
    "io.github.xiaodizi.config.Config$DiskOptimizationStrategy",
    "io.github.xiaodizi.config.Config$FlushCompression",
    "io.github.xiaodizi.config.Config$InternodeCompression",
    "io.github.xiaodizi.config.Config$MemtableAllocationType",
    "io.github.xiaodizi.config.Config$PaxosOnLinearizabilityViolation",
    "io.github.xiaodizi.config.Config$PaxosStatePurging",
    "io.github.xiaodizi.config.Config$PaxosVariant",
    "io.github.xiaodizi.config.Config$RepairCommandPoolFullStrategy",
    "io.github.xiaodizi.config.Config$UserFunctionTimeoutPolicy",
    "io.github.xiaodizi.config.Config$CorruptedTombstoneStrategy",
    "io.github.xiaodizi.config.DatabaseDescriptor$ByteUnit",
    "io.github.xiaodizi.config.DataRateSpec",
    "io.github.xiaodizi.config.DataRateSpec$DataRateUnit",
    "io.github.xiaodizi.config.DataRateSpec$DataRateUnit$1",
    "io.github.xiaodizi.config.DataRateSpec$DataRateUnit$2",
    "io.github.xiaodizi.config.DataRateSpec$DataRateUnit$3",
    "io.github.xiaodizi.config.DataStorageSpec",
    "io.github.xiaodizi.config.DataStorageSpec$DataStorageUnit",
    "io.github.xiaodizi.config.DataStorageSpec$DataStorageUnit$1",
    "io.github.xiaodizi.config.DataStorageSpec$DataStorageUnit$2",
    "io.github.xiaodizi.config.DataStorageSpec$DataStorageUnit$3",
    "io.github.xiaodizi.config.DataStorageSpec$DataStorageUnit$4",
    "io.github.xiaodizi.config.DataStorageSpec$IntBytesBound",
    "io.github.xiaodizi.config.DataStorageSpec$IntKibibytesBound",
    "io.github.xiaodizi.config.DataStorageSpec$IntMebibytesBound",
    "io.github.xiaodizi.config.DataStorageSpec$LongBytesBound",
    "io.github.xiaodizi.config.DataStorageSpec$LongMebibytesBound",
    "io.github.xiaodizi.config.DurationSpec",
    "io.github.xiaodizi.config.DataRateSpec$LongBytesPerSecondBound",
    "io.github.xiaodizi.config.DurationSpec$LongMillisecondsBound",
    "io.github.xiaodizi.config.DurationSpec$LongNanosecondsBound",
    "io.github.xiaodizi.config.DurationSpec$LongSecondsBound",
    "io.github.xiaodizi.config.DurationSpec$IntMillisecondsBound",
    "io.github.xiaodizi.config.DurationSpec$IntSecondsBound",
    "io.github.xiaodizi.config.DurationSpec$IntMinutesBound",
    "io.github.xiaodizi.config.EncryptionOptions",
    "io.github.xiaodizi.config.EncryptionOptions$ClientEncryptionOptions",
    "io.github.xiaodizi.config.EncryptionOptions$ServerEncryptionOptions",
    "io.github.xiaodizi.config.EncryptionOptions$ServerEncryptionOptions$InternodeEncryption",
    "io.github.xiaodizi.config.EncryptionOptions$ServerEncryptionOptions$OutgoingEncryptedPortSource",
    "io.github.xiaodizi.config.GuardrailsOptions",
    "io.github.xiaodizi.config.GuardrailsOptions$Config",
    "io.github.xiaodizi.config.GuardrailsOptions$ConsistencyLevels",
    "io.github.xiaodizi.config.GuardrailsOptions$TableProperties",
    "io.github.xiaodizi.config.ParameterizedClass",
    "io.github.xiaodizi.config.ReplicaFilteringProtectionOptions",
    "io.github.xiaodizi.config.YamlConfigurationLoader",
    "io.github.xiaodizi.config.YamlConfigurationLoader$PropertiesChecker",
    "io.github.xiaodizi.config.YamlConfigurationLoader$PropertiesChecker$1",
    "io.github.xiaodizi.config.YamlConfigurationLoader$CustomConstructor",
    "io.github.xiaodizi.config.TransparentDataEncryptionOptions",
    "io.github.xiaodizi.config.StartupChecksOptions",
    "io.github.xiaodizi.config.SubnetGroups",
    "io.github.xiaodizi.config.TrackWarnings",
    "io.github.xiaodizi.db.ConsistencyLevel",
    "io.github.xiaodizi.db.commitlog.CommitLogSegmentManagerFactory",
    "io.github.xiaodizi.db.commitlog.DefaultCommitLogSegmentMgrFactory",
    "io.github.xiaodizi.db.commitlog.AbstractCommitLogSegmentManager",
    "io.github.xiaodizi.db.commitlog.CommitLogSegmentManagerCDC",
    "io.github.xiaodizi.db.commitlog.CommitLogSegmentManagerStandard",
    "io.github.xiaodizi.db.commitlog.CommitLog",
    "io.github.xiaodizi.db.commitlog.CommitLogMBean",
    "io.github.xiaodizi.db.guardrails.GuardrailsConfig",
    "io.github.xiaodizi.db.guardrails.GuardrailsConfigMBean",
    "io.github.xiaodizi.db.guardrails.GuardrailsConfig$ConsistencyLevels",
    "io.github.xiaodizi.db.guardrails.GuardrailsConfig$TableProperties",
    "io.github.xiaodizi.db.guardrails.Values$Config",
    "io.github.xiaodizi.dht.IPartitioner",
    "io.github.xiaodizi.distributed.api.IInstance",
    "io.github.xiaodizi.distributed.api.IIsolatedExecutor",
    "io.github.xiaodizi.distributed.shared.InstanceClassLoader",
    "io.github.xiaodizi.distributed.impl.InstanceConfig",
    "io.github.xiaodizi.distributed.api.IInvokableInstance",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$CallableNoExcept",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$InstanceFunction",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$SerializableBiConsumer",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$SerializableBiFunction",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$SerializableCallable",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$SerializableConsumer",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$SerializableFunction",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$SerializableRunnable",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$SerializableTriFunction",
    "io.github.xiaodizi.distributed.impl.InvokableInstance$TriFunction",
    "io.github.xiaodizi.distributed.impl.Message",
    "io.github.xiaodizi.distributed.impl.NetworkTopology",
    "io.github.xiaodizi.exceptions.ConfigurationException",
    "io.github.xiaodizi.exceptions.RequestValidationException",
    "io.github.xiaodizi.exceptions.CassandraException",
    "io.github.xiaodizi.exceptions.InvalidRequestException",
    "io.github.xiaodizi.exceptions.TransportException",
    "io.github.xiaodizi.fql.FullQueryLogger",
    "io.github.xiaodizi.fql.FullQueryLoggerOptions",
    "io.github.xiaodizi.gms.IFailureDetector",
    "io.github.xiaodizi.locator.IEndpointSnitch",
    "io.github.xiaodizi.io.FSWriteError",
    "io.github.xiaodizi.io.FSError",
    "io.github.xiaodizi.io.compress.ICompressor",
    "io.github.xiaodizi.io.compress.ICompressor$Uses",
    "io.github.xiaodizi.io.compress.LZ4Compressor",
    "io.github.xiaodizi.io.sstable.metadata.MetadataType",
    "io.github.xiaodizi.io.util.BufferedDataOutputStreamPlus",
    "io.github.xiaodizi.io.util.RebufferingInputStream",
    "io.github.xiaodizi.io.util.FileInputStreamPlus",
    "io.github.xiaodizi.io.util.FileOutputStreamPlus",
    "io.github.xiaodizi.io.util.File",
    "io.github.xiaodizi.io.util.DataOutputBuffer",
    "io.github.xiaodizi.io.util.DataOutputBufferFixed",
    "io.github.xiaodizi.io.util.DataOutputStreamPlus",
    "io.github.xiaodizi.io.util.DataOutputPlus",
    "io.github.xiaodizi.io.util.DataInputPlus",
    "io.github.xiaodizi.io.util.DiskOptimizationStrategy",
    "io.github.xiaodizi.io.util.SpinningDiskOptimizationStrategy",
    "io.github.xiaodizi.io.util.PathUtils$IOToLongFunction",
    "io.github.xiaodizi.locator.Replica",
    "io.github.xiaodizi.locator.ReplicaCollection",
    "io.github.xiaodizi.locator.SimpleSeedProvider",
    "io.github.xiaodizi.locator.SeedProvider",
    "io.github.xiaodizi.security.ISslContextFactory",
    "io.github.xiaodizi.security.SSLFactory",
    "io.github.xiaodizi.security.EncryptionContext",
    "io.github.xiaodizi.service.CacheService$CacheType",
    "io.github.xiaodizi.transport.ProtocolException",
    "io.github.xiaodizi.utils.binlog.BinLogOptions",
    "io.github.xiaodizi.utils.FBUtilities",
    "io.github.xiaodizi.utils.FBUtilities$1",
    "io.github.xiaodizi.utils.CloseableIterator",
    "io.github.xiaodizi.utils.Pair",
    "io.github.xiaodizi.utils.concurrent.UncheckedInterruptedException",
    "io.github.xiaodizi.ConsoleAppender",
    "io.github.xiaodizi.ConsoleAppender$1",
    "io.github.xiaodizi.LogbackStatusListener",
    "io.github.xiaodizi.LogbackStatusListener$ToLoggerOutputStream",
    "io.github.xiaodizi.LogbackStatusListener$WrappedPrintStream",
    "io.github.xiaodizi.TeeingAppender",
    // generated classes
    "io.github.xiaodizi.config.ConfigBeanInfo",
    "io.github.xiaodizi.config.ConfigCustomizer",
    "io.github.xiaodizi.config.EncryptionOptionsBeanInfo",
    "io.github.xiaodizi.config.EncryptionOptionsCustomizer",
    "io.github.xiaodizi.config.EncryptionOptions$ServerEncryptionOptionsBeanInfo",
    "io.github.xiaodizi.config.EncryptionOptions$ServerEncryptionOptionsCustomizer",
    "io.github.xiaodizi.ConsoleAppenderBeanInfo",
    "io.github.xiaodizi.ConsoleAppenderCustomizer",
    "io.github.xiaodizi.locator.InetAddressAndPort",
    "io.github.xiaodizi.cql3.statements.schema.AlterKeyspaceStatement",
    "io.github.xiaodizi.cql3.statements.schema.CreateKeyspaceStatement"
    };

    static final Set<String> checkedClasses = new HashSet<>(Arrays.asList(validClasses));

    @Test
    @SuppressWarnings({"DynamicRegexReplaceableByCompiledPattern", "UseOfSystemOutOrSystemErr"})
    public void testDatabaseDescriptorRef() throws Throwable
    {
        PrintStream out = System.out;
        PrintStream err = System.err;

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        int threadCount = threads.getThreadCount();
        List<Long> existingThreadIDs = Arrays.stream(threads.getAllThreadIds()).boxed().collect(Collectors.toList());

        ClassLoader delegate = Thread.currentThread().getContextClassLoader();

        List<Pair<String, Exception>> violations = Collections.synchronizedList(new ArrayList<>());

        ClassLoader cl = new ClassLoader(null)
        {
            final Map<String, Class<?>> classMap = new HashMap<>();

            public URL getResource(String name)
            {
                return delegate.getResource(name);
            }

            public InputStream getResourceAsStream(String name)
            {
                return delegate.getResourceAsStream(name);
            }

            protected Class<?> findClass(String name) throws ClassNotFoundException
            {
                if (name.startsWith("java."))
                    // Java 11 does not allow a "custom" class loader (i.e. user code)
                    // to define classes in protected packages (like java, java.sql, etc).
                    // Therefore we have to delegate the call to the delegate class loader
                    // itself.
                    return delegate.loadClass(name);

                Class<?> cls = classMap.get(name);
                if (cls != null)
                    return cls;

                if (name.startsWith("io.github.xiaodizi."))
                {
                    // out.println(name);

                    if (!checkedClasses.contains(name))
                        violations.add(Pair.create(name, new Exception()));
                }

                URL url = delegate.getResource(name.replace('.', '/') + ".class");
                if (url == null)
                {
                    // For Java 11: system class files are not readable via getResource(), so
                    // try it this way
                    cls = Class.forName(name, false, delegate);
                    classMap.put(name, cls);
                    return cls;
                }

                // Java8 way + all non-system class files
                try (InputStream in = url.openConnection().getInputStream())
                {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    int c;
                    while ((c = in.read()) != -1)
                        os.write(c);
                    byte[] data = os.toByteArray();
                    cls = defineClass(name, data, 0, data.length);
                    classMap.put(name, cls);
                    return cls;
                }
                catch (IOException e)
                {
                    throw new ClassNotFoundException(name, e);
                }
            }
        };

        Thread.currentThread().setContextClassLoader(cl);

        assertEquals("thread started", threadCount, threads.getThreadCount());

        Class<?> databaseDescriptorClass = Class.forName("io.github.xiaodizi.config.DatabaseDescriptor", true, cl);

        // During DatabaseDescriptor instantiation some threads are spawned. We need to take them into account in
        // threadCount variable, otherwise they will be considered as new threads spawned by methods below. There is a
        // trick: in case of multiple runs of this test in the same JVM the number of such threads will be multiplied by
        // the number of runs. That's because DatabaseDescriptor is instantiated via a different class loader. So in
        // order to keep calculation logic correct, we ignore existing threads that were spawned during the previous
        // runs and change threadCount variable for the new threads only (if they have some specific names).
        for (ThreadInfo threadInfo : threads.getThreadInfo(threads.getAllThreadIds()))
        {
            // All existing threads have been already taken into account in threadCount variable, so we ignore them
            if (existingThreadIDs.contains(threadInfo.getThreadId()))
                continue;
            // Logback AsyncAppender thread needs to be taken into account
            if (threadInfo.getThreadName().equals("AsyncAppender-Worker-ASYNC"))
                threadCount++;
            // Logback basic threads need to be taken into account
            if (threadInfo.getThreadName().matches("logback-\\d+"))
                threadCount++;
            // Dynamic Attach thread needs to be taken into account, generally it is spawned by IDE
            if (threadInfo.getThreadName().equals("Attach Listener"))
                threadCount++;
        }

        for (String methodName : new String[]{
            "clientInitialization",
            "applyAddressConfig",
            "applyTokensConfig",
            // no seed provider in default configuration for clients
            // "applySeedProvider",
            // definitely not safe for clients - implicitly instantiates schema
            // "applyPartitioner",
            // definitely not safe for clients - implicitly instantiates StorageService
            // "applySnitch",
            "applyEncryptionContext",
            // starts "REQUEST-SCHEDULER" thread via RoundRobinScheduler
        })
        {
            Method method = databaseDescriptorClass.getDeclaredMethod(methodName);
            method.invoke(null);

            if (threadCount != threads.getThreadCount())
            {
                for (ThreadInfo threadInfo : threads.getThreadInfo(threads.getAllThreadIds()))
                    out.println("Thread #" + threadInfo.getThreadId() + ": " + threadInfo.getThreadName());
                assertEquals("thread started in " + methodName, threadCount, ManagementFactory.getThreadMXBean().getThreadCount());
            }

            checkViolations(err, violations);
        }
    }

    private void checkViolations(PrintStream err, List<Pair<String, Exception>> violations)
    {
        if (!violations.isEmpty())
        {
            StringBuilder sb = new StringBuilder();
            for (Pair<String, Exception> violation : new ArrayList<>(violations))
                sb.append("\n\n")
                  .append("VIOLATION: ").append(violation.left).append('\n')
                  .append(Throwables.getStackTraceAsString(violation.right));
            String msg = sb.toString();
            err.println(msg);

            fail(msg);
        }
    }
}
