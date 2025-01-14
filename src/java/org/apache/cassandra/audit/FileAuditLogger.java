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

package org.apache.cassandra.audit;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous, file-based audit logger; just uses the standard logging mechansim.
 * 同步的、基于文件的审计记录器；只使用标准的日志机制。写入文本文件
 */
public class FileAuditLogger implements IAuditLogger
{
    protected static final Logger logger = LoggerFactory.getLogger(FileAuditLogger.class);

    private volatile boolean enabled;

    public FileAuditLogger(Map<String, String> params)
    {
        enabled = true;
    }

    @Override
    public boolean isEnabled()
    {
        return enabled;
    }

    @Override
    public void log(AuditLogEntry auditLogEntry)
    {
        // don't bother with the volatile read of enabled here. just go ahead and log, other components
        // will check the enbaled field.
        // 不要为这里启用的volatile读取而烦恼。只需继续进行日志记录，其他组件就会检查enbaled字段。
        logger.info(auditLogEntry.getLogString());
    }

    @Override
    public void stop()
    {
        enabled = false;
    }
}
