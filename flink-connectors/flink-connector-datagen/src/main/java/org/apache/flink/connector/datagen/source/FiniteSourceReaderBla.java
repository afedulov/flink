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

package org.apache.flink.connector.datagen.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReaderBase;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;

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
@Public
public class FiniteSourceReaderBla<
                E, O, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
        extends IteratorSourceReaderBase<E, O, IterT, SplitT> {

    private final GeneratorFunction<E, O> generatorFunction;

    private final int elementsPerCycle;
    private final int snapshotsBetweenCycles;
    private int snapshotsCompleted;

    public FiniteSourceReaderBla(
            SourceReaderContext context,
            GeneratorFunction<E, O> generatorFunction,
            int elementsPerCycle,
            int snapshotsBetween) {
        super(context);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.elementsPerCycle = elementsPerCycle;
        this.snapshotsBetweenCycles = snapshotsBetween;
    }

    // ------------------------------------------------------------------------

    @Override
    public void start(SourceReaderContext context) {
        try {
            generatorFunction.open(context);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the GeneratorFunction", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<O> output) {
        snapshotsCompleted = 0;
        System.out.println("pollNext in " + Thread.currentThread());
        if (iterator != null) {
            if (iterator.hasNext()) {
                emitElements(output);
                return InputStatus.MORE_AVAILABLE;
            } else {
                finishSplit();
            }
        }

        final InputStatus inputStatus = tryMoveToNextSplit();
        if (inputStatus == InputStatus.MORE_AVAILABLE) {
            emitElements(output);
            availability = new CompletableFuture<>();
            return InputStatus.NOTHING_AVAILABLE;
        }
        return inputStatus;
    }

    private InputStatus emitElements(ReaderOutput<O> output) {
        for (int i = 0; i < elementsPerCycle; i++) {
            if (iterator.hasNext()) {
                output.collect(convert(iterator.next()));
            } else {
                return InputStatus.NOTHING_AVAILABLE;
            }
        }
        return InputStatus.MORE_AVAILABLE;
    }

    protected O convert(E value) {
        try {
            return generatorFunction.map(value);
        } catch (Exception e) {
            String message =
                    String.format(
                            "A user-provided generator function threw an exception on this input: %s",
                            value.toString());
            throw new FlinkRuntimeException(message, e);
        }
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        System.out.println(
                "@@@ SourceReader.snapshotState("
                        + checkpointId
                        + ") in Thread "
                        + Thread.currentThread());
        snapshotsCompleted++;
        System.out.println("@@@ snapshotsCompleted: " + snapshotsCompleted);
        if (snapshotsCompleted == snapshotsBetweenCycles) {
            availability.complete(null);
        }
        return super.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {}
}
