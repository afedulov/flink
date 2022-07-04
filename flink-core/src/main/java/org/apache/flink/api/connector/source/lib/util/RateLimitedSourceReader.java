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

package org.apache.flink.api.connector.source.lib.util;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.io.ratelimiting.RateLimiter;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Experimental
public class RateLimitedSourceReader<E, SplitT extends SourceSplit>
        implements SourceReader<E, SplitT> {

    private final SourceReader<E, SplitT> sourceReader;
    private final RateLimiter rateLimiter;

    public RateLimitedSourceReader(SourceReader<E, SplitT> sourceReader, RateLimiter rateLimiter) {
        checkNotNull(sourceReader);
        checkNotNull(rateLimiter);
        this.sourceReader = sourceReader;
        this.rateLimiter = rateLimiter;
    }

    // ------------------------------------------------------------------------

    @Override
    public void start() {
        sourceReader.start();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<E> output) throws Exception {
        rateLimiter.acquire();
        return sourceReader.pollNext(output);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return sourceReader.isAvailable();
    }

    @Override
    public void addSplits(List<SplitT> splits) {
        sourceReader.addSplits(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        sourceReader.notifyNoMoreSplits();
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        return sourceReader.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        sourceReader.close();
    }
}
