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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DataGeneratorSource}. */
public class DataGeneratorSourceTest {

    @Test
    @DisplayName("Correctly restores SplitEnumerator from a snapshot.")
    public void testRestoreEnumerator() throws Exception {
        final GeneratorFunction<Long, Long> generatorFunctionStateless = index -> index;
        final DataGeneratorSource<Long> dataGeneratorSource =
                new DataGeneratorSource<>(generatorFunctionStateless, 100, Types.LONG);

        final int parallelism = 2;
        final MockSplitEnumeratorContext<NumberSequenceSource.NumberSequenceSplit> context =
                new MockSplitEnumeratorContext<>(parallelism);

        SplitEnumerator<
                        NumberSequenceSource.NumberSequenceSplit,
                        Collection<NumberSequenceSource.NumberSequenceSplit>>
                enumerator = dataGeneratorSource.createEnumerator(context);

        // start() is not strictly necessary in the current implementation, but should logically be
        // executed in this order (protect against any breaking changes in the start() method).
        enumerator.start();

        Collection<NumberSequenceSource.NumberSequenceSplit> enumeratorState =
                enumerator.snapshotState(0);

        @SuppressWarnings("unchecked")
        final Queue<NumberSequenceSource.NumberSequenceSplit> splits =
                (Queue<NumberSequenceSource.NumberSequenceSplit>)
                        Whitebox.getInternalState(enumerator, "remainingSplits");

        assertThat(splits).hasSize(parallelism);

        enumerator = dataGeneratorSource.restoreEnumerator(context, enumeratorState);

        @SuppressWarnings("unchecked")
        final Queue<NumberSequenceSource.NumberSequenceSplit> restoredSplits =
                (Queue<NumberSequenceSource.NumberSequenceSplit>)
                        Whitebox.getInternalState(enumerator, "remainingSplits");

        assertThat(restoredSplits).hasSize(enumeratorState.size());
    }

    @Test
    @DisplayName("Uses the underlying NumberSequenceSource correctly for checkpointing.")
    public void testReaderCheckpoints() throws Exception {
        final long from = 177;
        final long mid = 333;
        final long to = 563;
        final long elementsPerCycle = (to - from) / 3;

        final TestingReaderOutput<Long> out = new TestingReaderOutput<>();

        SourceReader<Long, NumberSequenceSource.NumberSequenceSplit> reader = createReader();
        reader.addSplits(
                Arrays.asList(
                        new NumberSequenceSource.NumberSequenceSplit("split-1", from, mid),
                        new NumberSequenceSource.NumberSequenceSplit("split-2", mid + 1, to)));

        long remainingInCycle = elementsPerCycle;
        while (reader.pollNext(out) != InputStatus.END_OF_INPUT) {
            if (--remainingInCycle <= 0) {
                remainingInCycle = elementsPerCycle;
                // checkpoint
                List<NumberSequenceSource.NumberSequenceSplit> splits = reader.snapshotState(1L);

                // re-create and restore
                reader = createReader();
                if (splits.isEmpty()) {
                    reader.notifyNoMoreSplits();
                } else {
                    reader.addSplits(splits);
                }
            }
        }

        final List<Long> result = out.getEmittedRecords();
        validateSequence(result, from, to);
    }

    private static void validateSequence(
            final List<Long> sequence, final long from, final long to) {
        if (sequence.size() != to - from + 1) {
            failSequence(sequence, from, to);
        }

        long nextExpected = from;
        for (Long next : sequence) {
            if (next != nextExpected++) {
                failSequence(sequence, from, to);
            }
        }
    }

    private static void failSequence(final List<Long> sequence, final long from, final long to) {
        Assertions.fail(
                String.format(
                        "Expected: A sequence [%d, %d], but found: sequence (size %d) : %s",
                        from, to, sequence.size(), sequence));
    }

    private static SourceReader<Long, NumberSequenceSource.NumberSequenceSplit> createReader() {
        // the arguments passed in the source constructor matter only to the enumerator
        GeneratorFunction<Long, Long> generatorFunctionStateless = index -> index;
        DataGeneratorSource<Long> dataGeneratorSource =
                new DataGeneratorSource<>(generatorFunctionStateless, Long.MAX_VALUE, Types.LONG);

        return dataGeneratorSource.createReader(new DummyReaderContext());
    }

    // ------------------------------------------------------------------------
    //  test utils / mocks
    //
    //  the "flink-connector-test-utils module has proper mocks and utils,
    //  but cannot be used here, because it would create a cyclic dependency.
    // ------------------------------------------------------------------------

    private static final class DummyReaderContext implements SourceReaderContext {

        @Override
        public SourceReaderMetricGroup metricGroup() {
            return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
        }

        @Override
        public int currentParallelism() {
            return 1;
        }
    }

    private static final class TestingReaderOutput<E> implements ReaderOutput<E> {

        private final ArrayList<E> emittedRecords = new ArrayList<>();

        @Override
        public void collect(E record) {
            emittedRecords.add(record);
        }

        @Override
        public void collect(E record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOutput<E> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}

        public ArrayList<E> getEmittedRecords() {
            return emittedRecords;
        }
    }
}
