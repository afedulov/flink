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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.lib.util.GeneratorSourceReaderFactory;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A data source that produces N data points in parallel. This source is useful for testing and for
 * cases that just need a stream of N events of any kind.
 *
 * <p>The source splits the sequence into as many parallel sub-sequences as there are parallel
 * source readers.
 *
 * <p>Users can supply a {@code GeneratorFunction} for mapping the (sub-)sequences of Long values
 * into the generated events. For instance, the following code will produce the sequence of
 * ["Number: 0", "Number: 2", ... , "Number: 999"] elements.
 *
 * <pre>{@code
 * GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
 *
 * DataGeneratorSource<String> source =
 *         new DataGeneratorSource<>(generatorFunction, 1000, Types.STRING);
 *
 * DataStreamSource<String> stream =
 *         env.fromSource(source,
 *         WatermarkStrategy.noWatermarks(),
 *         "Generator Source");
 * }</pre>
 *
 * <p>The order of elements depends on the parallelism. Each sub-sequence will be produced in order.
 * Consequently, if the parallelism is limited to one, this will produce one sequence in order from
 * "Number: 0" to "Number: 999".
 *
 * <p>Note that this approach also makes it possible to produce deterministic watermarks at the
 * source based on the generated events and a custom {@code WatermarkStrategy}.
 *
 * <p>This source has built-in support for rate limiting. The following code will produce an
 * effectively unbounded (Long.MAX_VALUE from practical perspective will never be reached) stream of
 * Long values at the overall source rate (across all source subtasks) of 100 events per second.
 *
 * <pre>{@code
 * GeneratorFunction<Long, Long> generatorFunction = index -> index;
 *
 * DataGeneratorSource<String> source =
 *         new DataGeneratorSource<>(generatorFunctionStateless, Long.MAX_VALUE, 100, Types.STRING);
 * }</pre>
 *
 * <p>For more sophisticates use cases, users can take full control of the low-level data generation
 * details by supplying a custom {@code SourceReaderFactory}. The instantiated {@code SourceReader}s
 * are expected to produce data based on processing {@code NumberSequenceSplit}s. A customized
 * generator could, for instance, synchronize the data release process with checkpointing by making
 * use of ({@link SourceReader#notifyCheckpointComplete(long)}). Such functionality could be
 * helpful, for instance, for testing sinks that are expected to create specific metadata upon the
 * arrival of a checkpoint barrier and other similar use cases.
 *
 * <p>This source is always bounded. For very long sequences (for example when the {@code count} is
 * set to Long.MAX_VALUE), users may want to consider executing the application in a streaming
 * manner, because, despite the fact that the produced stream is bounded, the end bound is pretty
 * far away.
 */
@Experimental
public class DataGeneratorSource<OUT>
        implements Source<OUT, NumberSequenceSplit, Collection<NumberSequenceSplit>>,
                ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final SourceReaderFactory<OUT, NumberSequenceSplit> sourceReaderFactory;
    private final TypeInformation<OUT> typeInfo;

    private final NumberSequenceSource numberSource;

    /**
     * Instantiates a new {@code DataGeneratorSource}.
     *
     * @param generatorFunction The {@code GeneratorFunction} function.
     * @param count The number of generated data points.
     * @param typeInfo The type of the produced data points.
     */
    public DataGeneratorSource(
            GeneratorFunction<Long, OUT> generatorFunction,
            long count,
            TypeInformation<OUT> typeInfo) {
        this(generatorFunction, count, -1, typeInfo);
    }

    /**
     * Instantiates a new {@code DataGeneratorSource}.
     *
     * @param generatorFunction The {@code GeneratorFunction} function.
     * @param count The number of generated data points.
     * @param sourceRatePerSecond The overall source rate per second (across all source subtasks).
     * @param typeInfo The type of the produced data points.
     */
    public DataGeneratorSource(
            GeneratorFunction<Long, OUT> generatorFunction,
            long count,
            double sourceRatePerSecond,
            TypeInformation<OUT> typeInfo) {
        this(
                new GeneratorSourceReaderFactory<>(generatorFunction, sourceRatePerSecond),
                count,
                typeInfo);
        ClosureCleaner.clean(
                generatorFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
    }

    /**
     * Instantiates a new {@code DataGeneratorSource}. This constructor allows users can take
     * control of the low-level data generation details by supplying a custom {@code
     * SourceReaderFactory}. The instantiated {@code SourceReader}s are expected to produce data
     * based on processing {@code NumberSequenceSplit}s.
     *
     * @param sourceReaderFactory The {@link SourceReader} factory.
     * @param count The number of generated data points.
     * @param typeInfo The type of the produced data points.
     */
    public DataGeneratorSource(
            SourceReaderFactory<OUT, NumberSequenceSplit> sourceReaderFactory,
            long count,
            TypeInformation<OUT> typeInfo) {
        this.sourceReaderFactory = checkNotNull(sourceReaderFactory);
        this.typeInfo = checkNotNull(typeInfo);
        this.numberSource = new NumberSequenceSource(0, count - 1);
    }

    /** @return The number of records produced by this source. */
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
    public SourceReader<OUT, NumberSequenceSplit> createReader(SourceReaderContext readerContext) {
        return sourceReaderFactory.newSourceReader(readerContext);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> restoreEnumerator(
            SplitEnumeratorContext<NumberSequenceSplit> enumContext,
            Collection<NumberSequenceSplit> checkpoint) {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> createEnumerator(
            final SplitEnumeratorContext<NumberSequenceSplit> enumContext) {
        final List<NumberSequenceSplit> splits =
                numberSource.splitNumberRange(0, getCount(), enumContext.currentParallelism());
        return new IteratorSourceEnumerator<>(enumContext, splits);
    }

    @Override
    public SimpleVersionedSerializer<NumberSequenceSplit> getSplitSerializer() {
        return numberSource.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NumberSequenceSplit>>
            getEnumeratorCheckpointSerializer() {
        return numberSource.getEnumeratorCheckpointSerializer();
    }
}