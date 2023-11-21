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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReaderBase;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SourceReader} that synchronizes emission of N elements on the arrival of the checkpoint
 * barriers. This is possible because {@code pollNext} and {@code snapshotState} are executed in the
 * same thread and the fact that {@code pollNext} emits N elements at once. This reader is meant to
 * used solely for testing purposes as the substitution for the {@code FiniteTestSource} that is
 * based on the pre-FLIP-27 API.
 */
@Experimental
public class SourceReaderWithSnapshotsLatch<
                E, O, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
        extends IteratorSourceReaderBase<E, O, IterT, SplitT> {

    private final GeneratorFunction<E, O> generatorFunction;

    private BooleanSupplier couldExit;
    private int snapshotsCompleted;
    private int snapshotsToWaitFor = Integer.MAX_VALUE;
    private boolean done;

    public SourceReaderWithSnapshotsLatch(
            SourceReaderContext context,
            GeneratorFunction<E, O> generatorFunction,
            int snapshotsBetween,
            @Nullable BooleanSupplier couldExit) {
        super(context);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.couldExit = couldExit;
    }

    public SourceReaderWithSnapshotsLatch(
            SourceReaderContext context,
            GeneratorFunction<E, O> generatorFunction,
            int snapshotsBetween) {
        super(context);
        this.generatorFunction = checkNotNull(generatorFunction);
    }

    // ------------------------------------------------------------------------

    @Override
    public void start(SourceReaderContext context) {
        System.out.println("!!! Start" + " in Thread " + Thread.currentThread());
        try {
            generatorFunction.open(context);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the GeneratorFunction", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<O> output) {
        System.out.println(">>> pollNext" + " in Thread " + Thread.currentThread());
        // This is the termination path after the split was emitted twice. Always waiting for two
        // checkpoints after the emission.
        if (done) {
            if (couldExit != null) {
                System.out.println(">>> In Thread " + Thread.currentThread());
                return couldExit.getAsBoolean()
                        ? InputStatus.END_OF_INPUT
                        : InputStatus.NOTHING_AVAILABLE;
            } else {
                System.out.println("@@@ END_OF_INPUT" + " in Thread " + Thread.currentThread());
                // This stabilizes compaction tests
                //                try {
                //                    Thread.sleep(500);
                //                } catch (InterruptedException e) {
                //                    throw new RuntimeException(e);
                //                }
                return InputStatus.END_OF_INPUT;
            }
        }
        // This is the initial path
        if (currentSplit == null) {
            InputStatus inputStatus = tryMoveToNextSplit();
            switch (inputStatus) {
                case MORE_AVAILABLE:
                    emitElements(output);
                    snapshotsToWaitFor = 2;
                    snapshotsCompleted = 0;
                    break;
                case END_OF_INPUT:
                    // This can happen if source parallelism is larger than the number of available
                    // splits
                    return inputStatus;
            }
        } else {
            // Reusing the same split to emit elements the second time
            emitElements(output);
            snapshotsToWaitFor = 2;
            snapshotsCompleted = 0;
            done = true;
        }
        availability = new CompletableFuture<>();
        return InputStatus.NOTHING_AVAILABLE;
    }

    private void emitElements(ReaderOutput<O> output) {
        iterator = currentSplit.getIterator();
        //        for (int i = 0; i < elementsPerCycle; i++) {
        //        System.out.println("Iterator has next: " + iterator.hasNext());
        System.out.println("Split:" + currentSplit + ":" + " in Thread " + Thread.currentThread());
        while (iterator.hasNext()) {
            E next = iterator.next();
            O converted = convert(next);
            //            Thread.sleep(1);
            System.out.println(
                    ">> next: " + next + "->" + converted + " in Thread " + Thread.currentThread());
            output.collect(converted);
        }
        System.out.println(
                "Iterator has next: "
                        + iterator.hasNext()
                        + " in Thread "
                        + Thread.currentThread());
        //        Thread.sleep(100);
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
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // This stabilizes compaction tests
        //        Thread.sleep(300);
        // TODO: we do not know whether pollNext or notifyCheckpointComplete happens first. See
        //  FiniteTestFunction implementation for better handling
        System.out.println(
                "@@@ SourceReader.notifyCheckpointComplete("
                        + checkpointId
                        + ") in Thread "
                        + Thread.currentThread());
        snapshotsCompleted++;
        System.out.println(
                "@@@ snapshotsCompleted: "
                        + snapshotsCompleted
                        + " in Thread "
                        + Thread.currentThread());
        if (snapshotsCompleted >= snapshotsToWaitFor) {
            availability.complete(null);
        }

        if (couldExit != null) {
            if (couldExit.getAsBoolean()) {
                availability.complete(null);
            }
        }
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        return super.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        System.out.println(">>> CLOSE! in Thread " + Thread.currentThread());
    }
}
