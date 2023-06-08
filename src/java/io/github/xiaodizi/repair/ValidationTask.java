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
package io.github.xiaodizi.repair;

import java.util.concurrent.ExecutionException;

import io.github.xiaodizi.exceptions.RepairException;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.repair.messages.RepairMessage;
import io.github.xiaodizi.repair.messages.ValidationRequest;
import io.github.xiaodizi.streaming.PreviewKind;
import io.github.xiaodizi.utils.MerkleTrees;
import io.github.xiaodizi.utils.concurrent.AsyncFuture;

import static io.github.xiaodizi.net.Verb.VALIDATION_REQ;

/**
 * ValidationTask sends {@link ValidationRequest} to a replica.
 * When a replica sends back message, task completes.
 */
public class ValidationTask extends AsyncFuture<TreeResponse> implements Runnable
{
    private final RepairJobDesc desc;
    private final InetAddressAndPort endpoint;
    private final int nowInSec;
    private final PreviewKind previewKind;
    
    private boolean active = true;

    public ValidationTask(RepairJobDesc desc, InetAddressAndPort endpoint, int nowInSec, PreviewKind previewKind)
    {
        this.desc = desc;
        this.endpoint = endpoint;
        this.nowInSec = nowInSec;
        this.previewKind = previewKind;
    }

    /**
     * Send ValidationRequest to replica
     */
    public void run()
    {
        RepairMessage.sendMessageWithFailureCB(new ValidationRequest(desc, nowInSec),
                                               VALIDATION_REQ,
                                               endpoint,
                                               this::tryFailure);
    }

    /**
     * Receive MerkleTrees from replica node.
     *
     * @param trees MerkleTrees that is sent from replica. Null if validation failed on replica node.
     */
    public synchronized void treesReceived(MerkleTrees trees)
    {
        if (trees == null)
        {
            active = false;
            tryFailure(RepairException.warn(desc, previewKind, "Validation failed in " + endpoint));
        }
        else if (active)
        {
            trySuccess(new TreeResponse(endpoint, trees));
        }
        else
        {
            // If the task has already been aborted, just release the possibly off-heap trees and move along.
            trees.release();
            trySuccess(null);
        }
    }

    /**
     * Release any trees already received by this task, and place it a state where any trees 
     * received subsequently will be properly discarded.
     */
    public synchronized void abort()
    {
        if (active) 
        {
            if (isDone())
            {
                try 
                {
                    // If we're done, this should return immediately.
                    TreeResponse response = get();
                    
                    if (response.trees != null)
                        response.trees.release();
                } 
                catch (InterruptedException e) 
                {
                    // Restore the interrupt.
                    Thread.currentThread().interrupt();
                } 
                catch (ExecutionException e) 
                {
                    // Do nothing here. If an exception was set, there were no trees to release.
                }
            }
            
            active = false;
        }
    }
    
    public synchronized boolean isActive()
    {
        return active;
    }
}
