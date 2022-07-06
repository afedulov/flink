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
import org.apache.flink.api.connector.source.lib.util.MappingIteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.RateLimitedSourceReader;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;
import java.util.List;

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
@Experimental
public class DataGeneratorSourceV3<OUT>
        implements Source<
                        OUT,
                        NumberSequenceSource.NumberSequenceSplit,
                        Collection<NumberSequenceSource.NumberSequenceSplit>>,
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
     * @param generatorFunction The generator function that receives index numbers and translates
     *     them into events of the output type.
     * @param count The number of events to be produced.
     * @param typeInfo The type information of the returned events.
     */
    public DataGeneratorSourceV3(
            MapFunction<Long, OUT> generatorFunction, long count, TypeInformation<OUT> typeInfo) {
        this.typeInfo = checkNotNull(typeInfo);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.numberSource = new NumberSequenceSource(0, count);
    }

    /**
     * Creates a new {@code DataGeneratorSource} that produces <code>count</code> records in
     * parallel.
     *
     * @param generatorFunction The generator function that receives index numbers and translates
     *     them into events of the output type.
     * @param count The number of events to be produced.
     * @param sourceRatePerSecond The maximum number of events per seconds that this generator aims
     *     to produce. This is a target number for the whole source and the individual parallel
     *     source instances automatically adjust their rate taking based on the {@code
     *     sourceRatePerSecond} and the source parallelism.
     * @param typeInfo The type information of the returned events.
     */
    public DataGeneratorSourceV3(
            MapFunction<Long, OUT> generatorFunction,
            long count,
            long sourceRatePerSecond,
            TypeInformation<OUT> typeInfo) {
        checkArgument(sourceRatePerSecond > 0, "maxPerSeconds has to be a positive number");
        this.typeInfo = checkNotNull(typeInfo);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.numberSource = new NumberSequenceSource(0, count);
        this.maxPerSecond = sourceRatePerSecond;
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
    public SourceReader<OUT, NumberSequenceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        if (maxPerSecond > 0) {
            int parallelism = readerContext.currentParallelism();
            RateLimiter rateLimiter = new GuavaRateLimiter(maxPerSecond, parallelism);
            return new RateLimitedSourceReader<>(
                    new MappingIteratorSourceReader<>(readerContext, generatorFunction),
                    rateLimiter);
        } else {
            return new MappingIteratorSourceReader<>(readerContext, generatorFunction);
        }
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> restoreEnumerator(
            SplitEnumeratorContext<NumberSequenceSplit> enumContext,
            Collection<NumberSequenceSplit> checkpoint)
            throws Exception {
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
