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

package io.github.xiaodizi.service.reads.repair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xiaodizi.db.ConsistencyLevel;
import io.github.xiaodizi.db.DecoratedKey;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.Mutation;
import io.github.xiaodizi.db.MutationExceededMaxSizeException;
import io.github.xiaodizi.db.partitions.PartitionUpdate;
import io.github.xiaodizi.exceptions.ReadTimeoutException;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.net.MessagingService;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.tracing.Tracing;

import static io.github.xiaodizi.db.IMutation.MAX_MUTATION_SIZE;

public class BlockingReadRepairs
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepairs.class);

    private static final boolean DROP_OVERSIZED_READ_REPAIR_MUTATIONS =
        Boolean.getBoolean("cassandra.drop_oversized_readrepair_mutations");

    /**
     * Create a read repair mutation from the given update, if the mutation is not larger than the maximum
     * mutation size, otherwise return null. Or, if we're configured to be strict, throw an exception.
     */
    public static Mutation createRepairMutation(PartitionUpdate update, ConsistencyLevel consistency, InetAddressAndPort destination, boolean suppressException)
    {
        if (update == null)
            return null;

        DecoratedKey key = update.partitionKey();
        Mutation mutation = new Mutation(update);
        int messagingVersion = MessagingService.instance().versions.get(destination);

        try
        {
            mutation.validateSize(messagingVersion, 0);
            return mutation;
        }
        catch (MutationExceededMaxSizeException e)
        {
            Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
            TableMetadata metadata = update.metadata();

            if (DROP_OVERSIZED_READ_REPAIR_MUTATIONS)
            {
                logger.debug("Encountered an oversized ({}/{}) read repair mutation for table {}, key {}, node {}",
                             e.mutationSize,
                             MAX_MUTATION_SIZE,
                             metadata,
                             metadata.partitionKeyType.getString(key.getKey()),
                             destination);
            }
            else
            {
                logger.warn("Encountered an oversized ({}/{}) read repair mutation for table {}, key {}, node {}",
                            e.mutationSize,
                            MAX_MUTATION_SIZE,
                            metadata,
                            metadata.partitionKeyType.getString(key.getKey()),
                            destination);

                if (!suppressException)
                {
                    int blockFor = consistency.blockFor(keyspace.getReplicationStrategy());
                    Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
                    throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
                }
            }
            return null;
        }
    }
}
