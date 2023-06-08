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

package io.github.xiaodizi.service.paxos.v1;
import io.github.xiaodizi.net.IVerbHandler;
import io.github.xiaodizi.net.Message;
import io.github.xiaodizi.net.MessagingService;
import io.github.xiaodizi.service.paxos.Commit;
import io.github.xiaodizi.service.paxos.PaxosState;
import io.github.xiaodizi.service.paxos.PrepareResponse;

public class PrepareVerbHandler implements IVerbHandler<Commit>
{
    public static PrepareVerbHandler instance = new PrepareVerbHandler();

    public static PrepareResponse doPrepare(Commit toPrepare)
    {
        return PaxosState.legacyPrepare(toPrepare);
    }

    public void doVerb(Message<Commit> message)
    {
        Message<PrepareResponse> reply = message.responseWith(doPrepare(message.payload));
        MessagingService.instance().send(reply, message.from());
    }
}
