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

package org.apache.second.indexers;

import com.google.common.base.Stopwatch;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.second.ElasticSecondaryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.*;

public class EsIndexer extends NoOpIndexer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSecondaryIndex.class);

    private final ElasticSecondaryIndex index;
    private final DecoratedKey key;
    private final int nowInSec;
    private final String id;
    private final boolean delete;

    private LinkedBlockingQueue<Row> rowLinkedBlockingQueue = new LinkedBlockingQueue<>();

    ExecutorService executorService = Executors.newFixedThreadPool(1);

    public EsIndexer(ElasticSecondaryIndex index, DecoratedKey key, int nowInSec, boolean withDelete) {
        this.key = key;
        this.nowInSec = nowInSec;
        this.index = index;
        this.id = ByteBufferUtil.bytesToHex(key.getKey());
        this.delete = withDelete;
    }


    @Override
    public void begin() {
    }


    @Override
    public void insertRow(Row row) {
        Stopwatch time = Stopwatch.createStarted();
        try {
            rowLinkedBlockingQueue.put(row);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.debug("{} insertRow {} took {}ms", index.index_name, id, time.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public void updateRow(Row oldRowData, Row newRowData) {
        System.out.println("update");
        Stopwatch time = Stopwatch.createStarted();
        index.index(key, newRowData, oldRowData, nowInSec);
        LOGGER.debug("{} updateRow {} took {}ms", index.index_name, id, time.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public void partitionDelete(DeletionTime deletionTime) {
        Stopwatch time = Stopwatch.createStarted();
        index.delete(key);
        LOGGER.debug("{} partitionDelete {} took {}ms", index.index_name, id, time.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public void finish() {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                commit();
            }
        });
    }

    @Override
    public void commit() {
        // 最后统一提交写入
        while (!rowLinkedBlockingQueue.isEmpty()) {
            Row row = rowLinkedBlockingQueue.poll();
            index.index(this.key, row, null, nowInSec);
        }
    }
}
