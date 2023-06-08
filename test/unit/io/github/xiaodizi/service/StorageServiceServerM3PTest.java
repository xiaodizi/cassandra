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

package io.github.xiaodizi.service;


import org.junit.BeforeClass;
import org.junit.Test;

import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.commitlog.CommitLog;
import io.github.xiaodizi.exceptions.ConfigurationException;
import io.github.xiaodizi.gms.Gossiper;
import io.github.xiaodizi.io.util.File;
import io.github.xiaodizi.locator.IEndpointSnitch;
import io.github.xiaodizi.locator.PropertyFileSnitch;

import static io.github.xiaodizi.ServerTestUtils.cleanup;
import static io.github.xiaodizi.ServerTestUtils.mkdirs;
import static org.junit.Assert.assertTrue;

public class StorageServiceServerM3PTest
{
    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        Keyspace.setInitialized();
    }

    @Test
    public void testRegularMode() throws ConfigurationException
    {
        mkdirs();
        cleanup();
        StorageService.instance.initServer(0);
        for (String path : DatabaseDescriptor.getAllDataFileLocations())
        {
            // verify that storage directories are there.
            assertTrue(new File(path).exists());
        }
        // a proper test would be to call decommission here, but decommission() mixes both shutdown and datatransfer
        // calls.  This test is only interested in the shutdown-related items which a properly handled by just
        // stopping the client.
        //StorageService.instance.decommission();
        StorageService.instance.stopClient();
    }
}
