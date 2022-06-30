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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.DataGeneratorSource.GeneratorSequenceSplit;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.api.connector.source.lib.util.NoOpRateLimiter;
import org.apache.flink.api.connector.source.lib.util.RateLimiter;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.NumberSequenceIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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
public class DataGeneratorSource<OUT>
        implements Source<
                        OUT, GeneratorSequenceSplit<OUT>, Collection<GeneratorSequenceSplit<OUT>>>,
                ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorSource.class);

    private static final long serialVersionUID = 1L;

    private final TypeInformation<OUT> typeInfo;

    private final MapFunction<Long, OUT> generatorFunction;

    /** The end Generator in the sequence, inclusive. */
    private final NumberSequenceSource numberSource;

    private RateLimiter rateLimiter = new NoOpRateLimiter();

    /**
     * Creates a new {@code DataGeneratorSource} that produces <code>count</code> records in
     * parallel.
     *
     * @param typeInfo the type info
     * @param generatorFunction the generator function
     * @param count The count
     */
    public DataGeneratorSource(
            MapFunction<Long, OUT> generatorFunction, long count, TypeInformation<OUT> typeInfo) {
        this.typeInfo = checkNotNull(typeInfo);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.numberSource = new NumberSequenceSource(0, count);
    }

    public DataGeneratorSource(
            MapFunction<Long, OUT> generatorFunction,
            long count,
            RateLimiter rateLimiter,
            TypeInformation<OUT> typeInfo) {
        //  TODO:      checkArgument(maxPerSecond > 0, "maxPerSeconds has to be a positive number");
        this.typeInfo = checkNotNull(typeInfo);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.numberSource = new NumberSequenceSource(0, count);
        this.rateLimiter = rateLimiter;
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
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<GeneratorSequenceSplit<OUT>, Collection<GeneratorSequenceSplit<OUT>>>
            createEnumerator(
                    final SplitEnumeratorContext<GeneratorSequenceSplit<OUT>> enumContext) {

        final int parallelism = enumContext.currentParallelism();
        final List<NumberSequenceSplit> splits =
                numberSource.splitNumberRange(0, getCount(), parallelism);

        return new IteratorSourceEnumerator<>(
                enumContext, wrapSplits(splits, generatorFunction, rateLimiter));
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
        return new SplitSerializer<>(
                numberSource.getSplitSerializer(), generatorFunction, rateLimiter);
    }

    @Override
    public SimpleVersionedSerializer<Collection<GeneratorSequenceSplit<OUT>>>
            getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer<>(
                numberSource.getEnumeratorCheckpointSerializer(), generatorFunction, rateLimiter);
    }

    // ------------------------------------------------------------------------
    //  splits & checkpoint
    // ------------------------------------------------------------------------
    public static class GeneratorSequenceIterator<T> implements Iterator<T> {

        private final MapFunction<Long, T> generatorFunction;
        private final NumberSequenceIterator numSeqIterator;
        private final RateLimiter rateLimiter;

        public GeneratorSequenceIterator(
                NumberSequenceIterator numSeqIterator,
                MapFunction<Long, T> generatorFunction,
                RateLimiter rateLimiter) {
            this.generatorFunction = generatorFunction;
            this.numSeqIterator = numSeqIterator;
            this.rateLimiter = rateLimiter;
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
                LOG.error("NEXT!");
                rateLimiter.acquire();
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
                NumberSequenceSplit numberSequenceSplit,
                MapFunction<Long, T> generatorFunction,
                RateLimiter rateLimiter) {
            this.numberSequenceSplit = numberSequenceSplit;
            this.generatorFunction = generatorFunction;
            this.rateLimiter = rateLimiter;
        }

        private final NumberSequenceSplit numberSequenceSplit;

        private final MapFunction<Long, T> generatorFunction;
        private final RateLimiter rateLimiter;

        public GeneratorSequenceIterator<T> getIterator() {
            return new GeneratorSequenceIterator<>(
                    numberSequenceSplit.getIterator(), generatorFunction, rateLimiter);
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
                    generatorFunction,
                    rateLimiter);
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
        private final RateLimiter rateLimiter;

        private SplitSerializer(
                SimpleVersionedSerializer<NumberSequenceSplit> numberSplitSerializer,
                MapFunction<Long, T> generatorFunction,
                RateLimiter rateLimiter) {
            this.numberSplitSerializer = numberSplitSerializer;
            this.generatorFunction = generatorFunction;
            this.rateLimiter = rateLimiter;
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
                    numberSplitSerializer.deserialize(version, serialized),
                    generatorFunction,
                    rateLimiter);
        }
    }

    private static final class CheckpointSerializer<T>
            implements SimpleVersionedSerializer<Collection<GeneratorSequenceSplit<T>>> {

        private final SimpleVersionedSerializer<Collection<NumberSequenceSplit>>
                numberCheckpointSerializer;
        private final MapFunction<Long, T> generatorFunction;
        private final RateLimiter throttler;

        public CheckpointSerializer(
                SimpleVersionedSerializer<Collection<NumberSequenceSplit>>
                        numberCheckpointSerializer,
                MapFunction<Long, T> generatorFunction,
                RateLimiter throttler) {
            this.numberCheckpointSerializer = numberCheckpointSerializer;
            this.generatorFunction = generatorFunction;
            this.throttler = throttler;
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
            return wrapSplits(numberSequenceSplits, generatorFunction, throttler);
        }
    }

    private static <T> List<GeneratorSequenceSplit<T>> wrapSplits(
            Collection<NumberSequenceSplit> numberSequenceSplits,
            MapFunction<Long, T> generatorFunction,
            RateLimiter throttler) {
        return numberSequenceSplits.stream()
                .map(split -> new GeneratorSequenceSplit<>(split, generatorFunction, throttler))
                .collect(Collectors.toList());
    }
}
