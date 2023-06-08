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

package io.github.xiaodizi.simulator.cluster;

import io.github.xiaodizi.schema.Schema;
import io.github.xiaodizi.simulator.systems.SimulatedActionTask;

import java.time.Duration;

import static io.github.xiaodizi.simulator.Action.Modifier.DISPLAY_ORIGIN;
import static io.github.xiaodizi.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.junit.Assert.assertTrue;

class OnInstanceSyncSchemaForBootstrap extends SimulatedActionTask
{
    public OnInstanceSyncSchemaForBootstrap(ClusterActions actions, int node)
    {
        super("Sync Schema on " + node, RELIABLE_NO_TIMEOUTS.with(DISPLAY_ORIGIN), RELIABLE_NO_TIMEOUTS, actions, actions.cluster.get(node),
              () -> assertTrue("schema is ready", Schema.instance.waitUntilReady(Duration.ofMinutes(10))));
    }
}
