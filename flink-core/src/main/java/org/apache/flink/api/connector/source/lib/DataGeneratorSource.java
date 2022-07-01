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
import org.apache.flink.api.common.io.ratelimiting.NoOpRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.RateLimiter;
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
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.NumberSequenceIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
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

    private long maxPerSecond = -1;

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
                enumContext, wrapSplits(splits, generatorFunction, maxPerSecond, parallelism));
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
        return new SplitSerializer<>(generatorFunction, maxPerSecond);
    }

    @Override
    public SimpleVersionedSerializer<Collection<GeneratorSequenceSplit<OUT>>>
            getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer<>(generatorFunction, maxPerSecond);
    }

    // ------------------------------------------------------------------------
    //  splits & checkpoint
    // ------------------------------------------------------------------------
    public static class GeneratorSequenceIterator<T> implements Iterator<T> {

        private final MapFunction<Long, T> generatorFunction;
        private final NumberSequenceIterator numSeqIterator;
        private RateLimiter rateLimiter = new NoOpRateLimiter();

        public GeneratorSequenceIterator(
                NumberSequenceIterator numSeqIterator,
                MapFunction<Long, T> generatorFunction,
                long maxPerSecond,
                int parallelism) {
            this.generatorFunction = generatorFunction;
            this.numSeqIterator = numSeqIterator;
            if (maxPerSecond > 0) {
                this.rateLimiter = new GuavaRateLimiter(maxPerSecond, parallelism);
            }
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
                long maxPerSecond,
                int parallelism) {
            this.numberSequenceSplit = numberSequenceSplit;
            this.generatorFunction = generatorFunction;
            this.maxPerSecond = maxPerSecond;
            this.parallelism = parallelism;
        }

        private final NumberSequenceSplit numberSequenceSplit;
        private final MapFunction<Long, T> generatorFunction;
        private final long maxPerSecond;
        private final int parallelism;

        public GeneratorSequenceIterator<T> getIterator() {
            return new GeneratorSequenceIterator<>(
                    numberSequenceSplit.getIterator(),
                    generatorFunction,
                    maxPerSecond,
                    parallelism);
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
                    maxPerSecond,
                    parallelism);
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

        private static final int CURRENT_VERSION = 1;

        private final MapFunction<Long, T> generatorFunction;
        private final long maxPerSecond;

        private SplitSerializer(MapFunction<Long, T> generatorFunction, long maxPerSecond) {
            this.generatorFunction = generatorFunction;
            this.maxPerSecond = maxPerSecond;
        }

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(GeneratorSequenceSplit<T> split) throws IOException {
            checkArgument(
                    split.getClass() == GeneratorSequenceSplit.class,
                    "cannot serialize subclasses");

            // We will serialize 2 longs (16 bytes) plus the UTF representation of the string (2 +
            // length)
            final DataOutputSerializer out =
                    new DataOutputSerializer(split.splitId().length() + 18);
            serializeV1(out, split);
            return out.getCopyOfBuffer();
        }

        @Override
        public GeneratorSequenceSplit<T> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return deserializeV1(in, generatorFunction, maxPerSecond);
        }

        static <T> void serializeV1(DataOutputView out, GeneratorSequenceSplit<T> split)
                throws IOException {
            serializeNumberSequenceSplit(out, split.numberSequenceSplit);
            out.writeInt(split.parallelism);
        }

        static void serializeNumberSequenceSplit(
                DataOutputView out, NumberSequenceSplit numberSequenceSplit) throws IOException {
            out.writeUTF(numberSequenceSplit.splitId());
            out.writeLong(numberSequenceSplit.from());
            out.writeLong(numberSequenceSplit.to());
        }

        static <T> GeneratorSequenceSplit<T> deserializeV1(
                DataInputView in, MapFunction<Long, T> generatorFunction, long maxPerSecond)
                throws IOException {
            NumberSequenceSplit numberSequenceSplit = deserializeNumberSequenceSplit(in);
            int parallelism = in.readInt();
            return new GeneratorSequenceSplit<>(
                    numberSequenceSplit, generatorFunction, maxPerSecond, parallelism);
        }

        private static NumberSequenceSplit deserializeNumberSequenceSplit(DataInputView in)
                throws IOException {
            return new NumberSequenceSplit(in.readUTF(), in.readLong(), in.readLong());
        }
    }

    private static final class CheckpointSerializer<T>
            implements SimpleVersionedSerializer<Collection<GeneratorSequenceSplit<T>>> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        private final MapFunction<Long, T> generatorFunction;
        private final long maxPerSecond;

        public CheckpointSerializer(MapFunction<Long, T> generatorFunction, long maxPerSecond) {
            this.generatorFunction = generatorFunction;
            this.maxPerSecond = maxPerSecond;
        }

        @Override
        public byte[] serialize(Collection<GeneratorSequenceSplit<T>> checkpoint)
                throws IOException {
            // Each split needs 2 longs (16 bytes) plus the UTG representation of the string (2 +
            // length).
            // Assuming at most 4 digit split IDs, 22 bytes per split avoids any intermediate array
            // resizing.
            // Plus four bytes for the length field.
            // Plus four bytes for the parallelism.
            final DataOutputSerializer out =
                    new DataOutputSerializer(checkpoint.size() * 22 + 4 + 4);
            out.writeInt(checkpoint.size());
            for (GeneratorSequenceSplit<T> split : checkpoint) {
                DataGeneratorSource.SplitSerializer.serializeNumberSequenceSplit(
                        out, split.numberSequenceSplit);
            }

            final Optional<GeneratorSequenceSplit<T>> aSplit = checkpoint.stream().findFirst();
            if (aSplit.isPresent()) {
                int parallelism = aSplit.get().parallelism;
                out.writeInt(parallelism);
            }

            return out.getCopyOfBuffer();
        }

        @Override
        public Collection<GeneratorSequenceSplit<T>> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            final int num = in.readInt();
            final List<NumberSequenceSplit> result = new ArrayList<>(num);
            for (int remaining = num; remaining > 0; remaining--) {
                result.add(SplitSerializer.deserializeNumberSequenceSplit(in));
            }
            final int parallelism = in.readInt();
            return wrapSplits(result, generatorFunction, maxPerSecond, parallelism);
        }
    }

    private static <T> List<GeneratorSequenceSplit<T>> wrapSplits(
            Collection<NumberSequenceSplit> numberSequenceSplits,
            MapFunction<Long, T> generatorFunction,
            long maxPerSecond,
            int parallelism) {
        return numberSequenceSplits.stream()
                .map(
                        split ->
                                new GeneratorSequenceSplit<>(
                                        split, generatorFunction, maxPerSecond, parallelism))
                .collect(Collectors.toList());
    }
}
