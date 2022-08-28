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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
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
 * source readers. Each sub-sequence will be produced in order. Consequently, if the parallelism is
 * limited to one, this will produce one sequence in order.
 *
 * <p>This source is always bounded. For very long sequences (for example over the entire domain of
 * long integer values), user may want to consider executing the application in a s treaming manner,
 * because, despite the fact that the produced stream is bounded, the end bound is pretty far away.
 */
@Experimental
public class DataGeneratorSourceV4<OUT>
        implements Source<OUT, NumberSequenceSplit, Collection<NumberSequenceSplit>>,
                ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final SourceReaderFactory<OUT, NumberSequenceSplit> sourceReaderFactory;
    private final TypeInformation<OUT> typeInfo;

    private final NumberSequenceSource numberSource;

    public DataGeneratorSourceV4(
            SourceReaderFactory<OUT, NumberSequenceSplit> sourceReaderFactory,
            long count,
            TypeInformation<OUT> typeInfo) {
        this.sourceReaderFactory = checkNotNull(sourceReaderFactory);
        this.typeInfo = checkNotNull(typeInfo);
        this.numberSource = new NumberSequenceSource(0, count);
    }

    public DataGeneratorSourceV4(
            GeneratorFunction<Long, OUT> generatorFunction,
            long count,
            long sourceRatePerSecond,
            TypeInformation<OUT> typeInfo) {
        this(
                new GeneratorSourceReaderFactory<>(generatorFunction, sourceRatePerSecond),
                count,
                typeInfo);
    }

    public DataGeneratorSourceV4(
            GeneratorFunction<Long, OUT> generatorFunction,
            long count,
            TypeInformation<OUT> typeInfo) {
        this(generatorFunction, count, -1, typeInfo);
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
    public SourceReader<OUT, NumberSequenceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return sourceReaderFactory.newSourceReader(readerContext);
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
