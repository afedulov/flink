package org.apache.flink.connector.datagen.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.apache.flink.util.Preconditions.checkArgument;

public class FiniteTestGeneratorSource<T> {

    public static <OUT> DataGeneratorSource<OUT> create(OUT... data) throws IOException {
        return create(Arrays.asList(data));
    }

    public static <OUT> DataGeneratorSource<OUT> create(Collection<OUT> data) throws IOException {
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

        GeneratorFunction<Long, OUT> generatorFunction =
                new FromElementsGeneratorFunction<>(
                        typeInfo.createSerializer(new ExecutionConfig()), data);

        //        long count = data.size() * 2L;
        long count = data.size();
        RateLimiter rateLimiter = new CheckpointsControlRateLimiter(data.size());
        //        return new DataGeneratorSource<>(
        //                generatorFunction, count, ignoredParallelism -> rateLimiter, typeInfo);
        return new DataGeneratorSource<>(generatorFunction, count, typeInfo);
    }

    private static class CheckpointsControlRateLimiter implements RateLimiter, Serializable {
        private final int capacityPerCycle;
        private int capacityLeft;
        private transient int numCheckpointsComplete;

        public CheckpointsControlRateLimiter(int capacityPerCycle) {
            checkArgument(capacityPerCycle > 0, "Capacity per cycle has to be a positive number.");
            this.capacityPerCycle = capacityPerCycle;
            this.capacityLeft = capacityPerCycle;
        }

        transient CompletableFuture<Void> gatingFuture = null;
        transient int emitted = 0;

        @Override
        public CompletionStage<Void> acquire() {
            System.out.println("!acquire()");
            if (emitted < capacityPerCycle) {
                gatingFuture = CompletableFuture.completedFuture(null);
                return gatingFuture.thenRun(() -> emitted += 1);
            }

            //            if (capacityLeft <= 0) {
            //                gatingFuture = new CompletableFuture<>();
            //            }
            return gatingFuture.thenRun(() -> capacityLeft -= 1);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            System.out.println(
                    ">>>> notifyCheckpointComplete: "
                            + checkpointId
                            + " "
                            + Thread.currentThread());
            numCheckpointsComplete++;
            capacityLeft = capacityPerCycle;
            gatingFuture.complete(null);
        }
    }
}
