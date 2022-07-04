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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.ratelimiting.GuavaRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.RateLimiter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.api.connector.source.lib.util.RateLimitedIteratorSourceReaderNew;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.NumberSequenceIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A data source that produces generators N data points in parallel. This source is useful for
 * testing and for cases that just need a stream of N events of any kind.
 *
 * <p>The source splits the sequence into as many parallel sub-sequences as there are parallel
 * source readers. Each sub-sequence will be produced in order. Consequently, if the parallelism is
 * limited to one, this will produce one sequence in order.
 *
 * <p>This source is always bounded. For very long sequences (for example over the entire domain of
 * long integer values), user may want to consider executing the application in a streaming manner,
 * because, despite the fact that the produced stream is bounded, the end bound is pretty far away.
 */
@Public
public class DataGeneratorSourceV2<OUT>
        implements Source<
                        OUT,
                        DataGeneratorSourceV2.GeneratorSequenceSplit<OUT>,
                        Collection<DataGeneratorSourceV2.GeneratorSequenceSplit<OUT>>>,
                ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final TypeInformation<OUT> typeInfo;

    public final MapFunction<Long, OUT> generatorFunction;

    /** The end Generator in the sequence, inclusive. */
    private final NumberSequenceSource numberSource;

    private long maxPerSecond = -1;

    /**
     * Creates a new {@code DataGeneratorSource} that produces <code>count</code> records in
     * parallel.
     *
     * @param typeInfo the type info
     * @param generatorFunction the generator function
     * @param count The count
     */
    public DataGeneratorSourceV2(
            MapFunction<Long, OUT> generatorFunction, long count, TypeInformation<OUT> typeInfo) {
        this.typeInfo = checkNotNull(typeInfo);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.numberSource = new NumberSequenceSource(0, count);
    }

    public DataGeneratorSourceV2(
            MapFunction<Long, OUT> generatorFunction,
            long count,
            long maxPerSecond,
            TypeInformation<OUT> typeInfo) {
        checkArgument(maxPerSecond > 0, "maxPerSeconds has to be a positive number");
        this.typeInfo = checkNotNull(typeInfo);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.numberSource = new NumberSequenceSource(0, count);
        this.maxPerSecond = maxPerSecond;
    }

    public long getCount() {
        return numberSource.getTo();
    }

    // ------------------------------------------------------------------------
    //  source methods
    // ------------------------------------------------------------------------

    @Override
    public TypeInformation<OUT> getProducedType() {
        return typeInfo;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<OUT, GeneratorSequenceSplit<OUT>> createReader(
            SourceReaderContext readerContext) {
        RateLimiter rateLimiter =
                new GuavaRateLimiter(maxPerSecond, readerContext.currentParallelism());
        return new RateLimitedIteratorSourceReaderNew<>(readerContext, rateLimiter);
    }

    @Override
    public SplitEnumerator<GeneratorSequenceSplit<OUT>, Collection<GeneratorSequenceSplit<OUT>>>
            createEnumerator(
                    final SplitEnumeratorContext<GeneratorSequenceSplit<OUT>> enumContext) {

        final List<NumberSequenceSplit> splits =
                numberSource.splitNumberRange(0, getCount(), enumContext.currentParallelism());
        return new IteratorSourceEnumerator<>(enumContext, wrapSplits(splits, generatorFunction));
    }

    @Override
    public SplitEnumerator<GeneratorSequenceSplit<OUT>, Collection<GeneratorSequenceSplit<OUT>>>
            restoreEnumerator(
                    final SplitEnumeratorContext<GeneratorSequenceSplit<OUT>> enumContext,
                    Collection<GeneratorSequenceSplit<OUT>> checkpoint) {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<GeneratorSequenceSplit<OUT>> getSplitSerializer() {
        return new SplitSerializer<>(numberSource.getSplitSerializer(), generatorFunction);
    }

    @Override
    public SimpleVersionedSerializer<Collection<GeneratorSequenceSplit<OUT>>>
            getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer<>(
                numberSource.getEnumeratorCheckpointSerializer(), generatorFunction);
    }

    // ------------------------------------------------------------------------
    //  splits & checkpoint
    // ------------------------------------------------------------------------
    public static class GeneratorSequenceIterator<T> implements Iterator<T> {

        private final MapFunction<Long, T> generatorFunction;
        private final NumberSequenceIterator numSeqIterator;

        public GeneratorSequenceIterator(
                NumberSequenceIterator numSeqIterator, MapFunction<Long, T> generatorFunction) {
            this.generatorFunction = generatorFunction;
            this.numSeqIterator = numSeqIterator;
        }

        @Override
        public boolean hasNext() {
            return numSeqIterator.hasNext();
        }

        public long getCurrent() {
            return numSeqIterator.getCurrent();
        }

        public long getTo() {
            return numSeqIterator.getTo();
        }

        @Override
        public T next() {
            try {
                return generatorFunction.map(numSeqIterator.next());
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Exception while generating element %d",
                                numSeqIterator.getCurrent()),
                        e);
            }
        }
    }

    /** A split of the source, representing a Generator sub-sequence. */
    public static class GeneratorSequenceSplit<T>
            implements IteratorSourceSplit<T, GeneratorSequenceIterator<T>> {

        public GeneratorSequenceSplit(
                NumberSequenceSplit numberSequenceSplit, MapFunction<Long, T> generatorFunction) {
            this.numberSequenceSplit = numberSequenceSplit;
            this.generatorFunction = generatorFunction;
        }

        private final NumberSequenceSplit numberSequenceSplit;

        private final MapFunction<Long, T> generatorFunction;

        public GeneratorSequenceIterator<T> getIterator() {
            return new GeneratorSequenceIterator<>(
                    numberSequenceSplit.getIterator(), generatorFunction);
        }

        @Override
        public String splitId() {
            return numberSequenceSplit.splitId();
        }

        @Override
        public IteratorSourceSplit<T, GeneratorSequenceIterator<T>> getUpdatedSplitForIterator(
                final GeneratorSequenceIterator<T> iterator) {
            return new GeneratorSequenceSplit<>(
                    (NumberSequenceSplit)
                            numberSequenceSplit.getUpdatedSplitForIterator(iterator.numSeqIterator),
                    generatorFunction);
        }

        @Override
        public String toString() {
            return String.format(
                    "GeneratorSequenceSplit [%d, %d] (%s)",
                    numberSequenceSplit.from(),
                    numberSequenceSplit.to(),
                    numberSequenceSplit.splitId());
        }
    }

    private static final class SplitSerializer<T>
            implements SimpleVersionedSerializer<GeneratorSequenceSplit<T>> {

        private final SimpleVersionedSerializer<NumberSequenceSplit> numberSplitSerializer;
        private final MapFunction<Long, T> generatorFunction;

        private SplitSerializer(
                SimpleVersionedSerializer<NumberSequenceSplit> numberSplitSerializer,
                MapFunction<Long, T> generatorFunction) {
            this.numberSplitSerializer = numberSplitSerializer;
            this.generatorFunction = generatorFunction;
        }

        @Override
        public int getVersion() {
            return numberSplitSerializer.getVersion();
        }

        @Override
        public byte[] serialize(GeneratorSequenceSplit<T> split) throws IOException {
            return numberSplitSerializer.serialize(split.numberSequenceSplit);
        }

        @Override
        public GeneratorSequenceSplit<T> deserialize(int version, byte[] serialized)
                throws IOException {
            return new GeneratorSequenceSplit<>(
                    numberSplitSerializer.deserialize(version, serialized), generatorFunction);
        }
    }

    private static final class CheckpointSerializer<T>
            implements SimpleVersionedSerializer<Collection<GeneratorSequenceSplit<T>>> {

        private final SimpleVersionedSerializer<Collection<NumberSequenceSplit>>
                numberCheckpointSerializer;
        private final MapFunction<Long, T> generatorFunction;

        public CheckpointSerializer(
                SimpleVersionedSerializer<Collection<NumberSequenceSplit>>
                        numberCheckpointSerializer,
                MapFunction<Long, T> generatorFunction) {
            this.numberCheckpointSerializer = numberCheckpointSerializer;
            this.generatorFunction = generatorFunction;
        }

        @Override
        public int getVersion() {
            return numberCheckpointSerializer.getVersion();
        }

        @Override
        public byte[] serialize(Collection<GeneratorSequenceSplit<T>> checkpoint)
                throws IOException {
            return numberCheckpointSerializer.serialize(
                    checkpoint.stream()
                            .map(split -> split.numberSequenceSplit)
                            .collect(Collectors.toList()));
        }

        @Override
        public Collection<GeneratorSequenceSplit<T>> deserialize(int version, byte[] serialized)
                throws IOException {
            Collection<NumberSequenceSplit> numberSequenceSplits =
                    numberCheckpointSerializer.deserialize(version, serialized);
            return wrapSplits(numberSequenceSplits, generatorFunction);
        }
    }

    private static <T> List<GeneratorSequenceSplit<T>> wrapSplits(
            Collection<NumberSequenceSplit> numberSequenceSplits,
            MapFunction<Long, T> generatorFunction) {
        return numberSequenceSplits.stream()
                .map(split -> new GeneratorSequenceSplit<>(split, generatorFunction))
                .collect(Collectors.toList());
    }
}
