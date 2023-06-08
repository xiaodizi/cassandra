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

package io.github.xiaodizi.distributed.mock.nodetool;

import com.google.common.collect.Multimap;
import io.github.xiaodizi.batchlog.BatchlogManager;
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.db.ColumnFamilyStoreMBean;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.compaction.CompactionManager;
import io.github.xiaodizi.gms.FailureDetector;
import io.github.xiaodizi.gms.FailureDetectorMBean;
import io.github.xiaodizi.gms.Gossiper;
import io.github.xiaodizi.hints.HintsService;
import io.github.xiaodizi.locator.DynamicEndpointSnitchMBean;
import io.github.xiaodizi.locator.EndpointSnitchInfo;
import io.github.xiaodizi.locator.EndpointSnitchInfoMBean;
import io.github.xiaodizi.metrics.CassandraMetricsRegistry;
import io.github.xiaodizi.net.MessagingService;
import io.github.xiaodizi.service.*;
import io.github.xiaodizi.streaming.StreamManager;
import io.github.xiaodizi.tools.NodeProbe;
import org.mockito.Mockito;

import javax.management.ListenerNotFoundException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.Map;

public class InternalNodeProbe extends NodeProbe
{
    private final boolean withNotifications;

    public InternalNodeProbe(boolean withNotifications)
    {
        this.withNotifications = withNotifications;
        connect();
    }

    protected void connect()
    {
        // note that we are not connecting via JMX for testing
        mbeanServerConn = null;
        jmxc = null;

        if (withNotifications)
        {
            ssProxy = StorageService.instance;
        }
        else
        {
            // replace the notification apis with a no-op method
            StorageServiceMBean mock = Mockito.spy(StorageService.instance);
            Mockito.doNothing().when(mock).addNotificationListener(Mockito.any(), Mockito.any(), Mockito.any());
            try
            {
                Mockito.doNothing().when(mock).removeNotificationListener(Mockito.any(), Mockito.any(), Mockito.any());
                Mockito.doNothing().when(mock).removeNotificationListener(Mockito.any());
            }
            catch (ListenerNotFoundException e)
            {
                throw new AssertionError(e);
            }
            ssProxy = mock;
        }
        msProxy = MessagingService.instance();
        streamProxy = StreamManager.instance;
        compactionProxy = CompactionManager.instance;
        fdProxy = (FailureDetectorMBean) FailureDetector.instance;
        cacheService = CacheService.instance;
        spProxy = StorageProxy.instance;
        hsProxy = HintsService.instance;

        gcProxy = new GCInspector();
        gossProxy = Gossiper.instance;
        bmProxy = BatchlogManager.instance;
        arsProxy = ActiveRepairService.instance;
        memProxy = ManagementFactory.getMemoryMXBean();
        runtimeProxy = ManagementFactory.getRuntimeMXBean();
    }

    @Override
    public void close()
    {
        // nothing to close. no-op
    }

    @Override
    // overrides all the methods referenced mbeanServerConn/jmxc in super
    public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy()
    {
        return new EndpointSnitchInfo();
    }

	@Override
    public DynamicEndpointSnitchMBean getDynamicEndpointSnitchInfoProxy()
    {
        return (DynamicEndpointSnitchMBean) DatabaseDescriptor.createEndpointSnitch(true, DatabaseDescriptor.getRawConfig().endpoint_snitch);
    }

    public CacheServiceMBean getCacheServiceMBean()
    {
        return cacheService;
    }

    @Override
    public ColumnFamilyStoreMBean getCfsProxy(String ks, String cf)
    {
        return Keyspace.open(ks).getColumnFamilyStore(cf);
    }

    // The below methods are only used by the commands (i.e. Info, TableHistogram, TableStats, etc.) that display informations. Not useful for dtest, so disable it.
    @Override
    public Object getCacheMetric(String cacheType, String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Multimap<String, String> getThreadPools()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getThreadPoolMetric(String pathName, String poolName, String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getColumnFamilyMetric(String ks, String cf, String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CassandraMetricsRegistry.JmxTimerMBean getProxyMetric(String scope)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CassandraMetricsRegistry.JmxTimerMBean getMessagingQueueWaitMetrics(String verb)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getCompactionMetric(String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getClientMetric(String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getStorageMetric(String metricName)
    {
        throw new UnsupportedOperationException();
    }
}
