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

package io.github.xiaodizi.distributed.impl;

import io.github.xiaodizi.diag.DiagnosticEventService;
import io.github.xiaodizi.distributed.api.IListen;
import io.github.xiaodizi.gms.GossiperEvent;
import io.github.xiaodizi.schema.SchemaEvent;

import java.util.function.Consumer;

public class Listen implements IListen
{
    final Instance instance;

    public Listen(Instance instance)
    {
        this.instance = instance;
    }

    public Cancel schema(Runnable onChange)
    {
        Consumer<SchemaEvent> consumer = event -> onChange.run();
        DiagnosticEventService.instance().subscribe(SchemaEvent.class, SchemaEvent.SchemaEventType.VERSION_UPDATED, consumer);
        return () -> DiagnosticEventService.instance().unsubscribe(SchemaEvent.class, consumer);
    }

    public Cancel liveMembers(Runnable onChange)
    {
        Consumer<GossiperEvent> consumer = event -> onChange.run();
        DiagnosticEventService.instance().subscribe(GossiperEvent.class, GossiperEvent.GossiperEventType.REAL_MARKED_ALIVE, consumer);
        return () -> DiagnosticEventService.instance().unsubscribe(GossiperEvent.class, consumer);
    }
}
