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

import io.github.xiaodizi.db.SinglePartitionReadCommand;
import io.github.xiaodizi.db.partitions.FilteredPartition;
import io.github.xiaodizi.db.partitions.PartitionUpdate;
import io.github.xiaodizi.exceptions.InvalidRequestException;
import io.github.xiaodizi.service.paxos.Ballot;

/**
 * Abstract the conditions and updates for a CAS operation.
 */
public interface CASRequest
{
    /**
     * The command to use to fetch the value to compare for the CAS.
     */
    public SinglePartitionReadCommand readCommand(int nowInSec);

    /**
     * Returns whether the provided CF, that represents the values fetched using the
     * readFilter(), match the CAS conditions this object stands for.
     */
    public boolean appliesTo(FilteredPartition current) throws InvalidRequestException;

    /**
     * The updates to perform of a CAS success. The values fetched using the readFilter()
     * are passed as argument.
     */
    public PartitionUpdate makeUpdates(FilteredPartition current, ClientState clientState, Ballot ballot) throws InvalidRequestException;
}
