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
/**
 * Hints subsystem consists of several components.
 *
 * {@link io.github.xiaodizi.hints.Hint} encodes all the required metadata and the mutation being hinted.
 *
 * {@link io.github.xiaodizi.hints.HintsBuffer} provides a temporary buffer for writing the hints to in a concurrent manner,
 * before we flush them to disk.
 *
 * {@link io.github.xiaodizi.hints.HintsBufferPool} is responsible for submitting {@link io.github.xiaodizi.hints.HintsBuffer}
 * instances for flushing when they exceed their capacity, and for maitaining a reserve {@link io.github.xiaodizi.hints.HintsBuffer}
 * instance, and creating extra ones if flushing cannot keep up with arrival rate.
 *
 * {@link io.github.xiaodizi.hints.HintsWriteExecutor} is a single-threaded executor that performs all the writing to disk.
 *
 * {@link io.github.xiaodizi.hints.HintsDispatchExecutor} is a multi-threaded executor responsible for dispatch of
 * the hints to their destinations.
 *
 * {@link io.github.xiaodizi.hints.HintsStore} tracks the state of all hints files (written and being written to)
 * for a given host id destination.
 *
 * {@link io.github.xiaodizi.hints.HintsCatalog} maintains the mapping of host ids to {@link io.github.xiaodizi.hints.HintsStore}
 * instances, and provides some aggregate APIs.
 *
 * {@link io.github.xiaodizi.hints.HintsService} wraps the catalog, the pool, and the two executors, acting as a front-end
 * for hints.
 */
package io.github.xiaodizi.hints;
