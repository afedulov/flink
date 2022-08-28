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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SourceReader} that returns the values of an iterator, supplied via an {@link
 * IteratorSourceSplit}.
 *
 * <p>The {@code IteratorSourceSplit} is also responsible for taking the current iterator and
 * turning it back into a split for checkpointing.
 *
 * @param <E> The type of events returned by the reader.
 * @param <IterT> The type of the iterator that produces the events. This type exists to make the
 *     conversion between iterator and {@code IteratorSourceSplit} type safe.
 * @param <SplitT> The concrete type of the {@code IteratorSourceSplit} that creates and converts
 *     the iterator that produces this reader's elements.
 */
@Experimental
public class RateLimitedIteratorSourceReader<
                E, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
        implements SourceReader<E, SplitT> {

    private final IteratorSourceReader<E, IterT, SplitT> iteratorSourceReader;
    private final RateLimiter rateLimiter;

    public RateLimitedIteratorSourceReader(
            SourceReaderContext readerContext, RateLimiter rateLimiter) {
        checkNotNull(readerContext);
        iteratorSourceReader = new IteratorSourceReader<>(readerContext);
        this.rateLimiter = rateLimiter;
    }

    // ------------------------------------------------------------------------

    @Override
    public void start() {
        iteratorSourceReader.start();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<E> output) throws InterruptedException {
        rateLimiter.acquire();
        return iteratorSourceReader.pollNext(output);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return iteratorSourceReader.isAvailable();
    }

    @Override
    public void addSplits(List<SplitT> splits) {
        iteratorSourceReader.addSplits(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        iteratorSourceReader.notifyNoMoreSplits();
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        return iteratorSourceReader.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        iteratorSourceReader.close();
    }
}
