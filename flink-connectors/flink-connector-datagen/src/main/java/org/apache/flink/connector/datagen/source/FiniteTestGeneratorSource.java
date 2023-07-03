package org.apache.flink.connector.datagen.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.apache.flink.util.Preconditions.checkArgument;

public class FiniteTestGeneratorSource<T> {

    public static <OUT> DataGeneratorSource<OUT> create(OUT... data) {
        return create(Arrays.asList(data));
    }

    public static <OUT> DataGeneratorSource<OUT> create(Collection<OUT> data) {
        GeneratorFunction<Long, OUT> generatorFunction = new FiniteTestGeneratorFunction<>(data);
        OUT first = data.iterator().next();
        if (first == null) {
            throw new IllegalArgumentException("Collection must not contain null elements");
        }

        TypeInformation<OUT> typeInfo;
        try {
            typeInfo = TypeExtractor.getForObject(first);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not create TypeInformation for type "
                            + first.getClass()
                            + "; please specify the TypeInformation manually via "
                            + "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)",
                    e);
        }
        long count = data.size() * 2L;
        RateLimiter rateLimiter = new CheckpointsControlRateLimiter(data.size());
        return new DataGeneratorSource<>(
                generatorFunction, count, ignoredParallelism -> rateLimiter, typeInfo);
    }

    private static class FiniteTestGeneratorFunction<OUT> implements GeneratorFunction<Long, OUT> {

        Iterator<OUT> elementsIterator;

        FiniteTestGeneratorFunction(Iterable<OUT> elements) {
            this.elementsIterator = elements.iterator();
        }

        @Override
        public OUT map(Long value) throws Exception {
            return elementsIterator.next();
        }
    }

    private static class CheckpointsControlRateLimiter implements RateLimiter {
        private final int capacityPerCycle;
        private int capacityLeft;
        private transient int numCheckpointsComplete;

        public CheckpointsControlRateLimiter(int capacityPerCycle) {
            checkArgument(capacityPerCycle > 0, "Capacity per cycle has to be a positive number.");
            this.capacityPerCycle = capacityPerCycle;
            this.capacityLeft = capacityPerCycle;
        }

        transient CompletableFuture<Void> gatingFuture = null;

        @Override
        public CompletionStage<Void> acquire() {
            if (gatingFuture == null) {
                gatingFuture = CompletableFuture.completedFuture(null);
            }
            if (capacityLeft <= 0) {
                gatingFuture = new CompletableFuture<>();
            }
            return gatingFuture.thenRun(() -> capacityLeft -= 1);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            numCheckpointsComplete++;
            capacityLeft = capacityPerCycle;
            gatingFuture.complete(null);
        }
    }
}
