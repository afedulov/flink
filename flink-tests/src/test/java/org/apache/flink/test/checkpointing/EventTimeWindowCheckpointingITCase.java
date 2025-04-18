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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.state.forst.ForStOptions;
import org.apache.flink.state.forst.ForStStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.checkpointing.utils.FailingSource;
import org.apache.flink.test.checkpointing.utils.IntType;
import org.apache.flink.test.checkpointing.utils.ValidatingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.test.checkpointing.EventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_INCREMENTAL_ZK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This verifies that checkpointing works correctly with event time windows. This is more strict
 * than {@link ProcessingTimeWindowCheckpointingITCase} because for event-time the contents of the
 * emitted windows are deterministic.
 *
 * <p>Split into multiple test classes in order to decrease the runtime per backend and not run into
 * CI infrastructure limits like no std output being emitted for I/O heavy variants.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class EventTimeWindowCheckpointingITCase extends TestLogger {

    private static final int MAX_MEM_STATE_SIZE = 20 * 1024 * 1024;
    private static final int PARALLELISM = 4;
    private static final int NUM_OF_TASK_MANAGERS = 2;

    private TestingServer zkServer;

    public MiniClusterWithClientResource miniClusterResource;

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule public TestName name = new TestName();

    private Configuration configuration;

    public StateBackendEnum stateBackendEnum;

    enum StateBackendEnum {
        MEM,
        FILE,
        ROCKSDB_FULL,
        ROCKSDB_INCREMENTAL,
        ROCKSDB_INCREMENTAL_ZK,
        FORST_INCREMENTAL
    }

    @Parameterized.Parameters(name = "statebackend type ={0}")
    public static Collection<Object[]> parameter() {
        return Arrays.stream(StateBackendEnum.values())
                .map((type) -> new Object[][] {{type}})
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }

    public EventTimeWindowCheckpointingITCase(StateBackendEnum stateBackendEnum) {
        this.stateBackendEnum = stateBackendEnum;
    }

    protected StateBackendEnum getStateBackend() {
        return this.stateBackendEnum;
    }

    protected final MiniClusterWithClientResource getMiniClusterResource() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumberTaskManagers(NUM_OF_TASK_MANAGERS)
                        .setNumberSlotsPerTaskManager(PARALLELISM / NUM_OF_TASK_MANAGERS)
                        .build());
    }

    private Configuration getConfigurationSafe() {
        try {
            return getConfiguration();
        } catch (Exception e) {
            throw new AssertionError("Could not initialize test.", e);
        }
    }

    private Configuration getConfiguration() throws Exception {

        // print a message when starting a test method to avoid Travis' <tt>"Maven produced no
        // output for xxx seconds."</tt> messages
        System.out.println(
                "Starting " + getClass().getCanonicalName() + "#" + name.getMethodName() + ".");

        // Testing HA Scenario / ZKCompletedCheckpointStore with incremental checkpoints
        StateBackendEnum stateBackendEnum = getStateBackend();
        if (ROCKSDB_INCREMENTAL_ZK.equals(stateBackendEnum)) {
            zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
        }

        Configuration config = createClusterConfig();

        switch (stateBackendEnum) {
            case MEM:
                config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
                config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
                break;
            case FILE:
                {
                    final File backups = tempFolder.newFolder().getAbsoluteFile();
                    config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
                    config.set(
                            CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                            Path.fromLocalFile(backups).toUri().toString());
                    break;
                }
            case ROCKSDB_FULL:
                {
                    setupRocksDB(config, -1, false);
                    break;
                }
            case ROCKSDB_INCREMENTAL:
                // Test RocksDB based timer service as well
                config.set(
                        RocksDBOptions.TIMER_SERVICE_FACTORY,
                        EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
                setupRocksDB(config, 16, true);
                break;
            case ROCKSDB_INCREMENTAL_ZK:
                {
                    setupRocksDB(config, 16, true);
                    break;
                }
            case FORST_INCREMENTAL:
                {
                    config.set(
                            ForStOptions.TIMER_SERVICE_FACTORY,
                            ForStStateBackend.PriorityQueueStateType.ForStDB);
                    setupForSt(config, 16);
                    break;
                }
            default:
                throw new IllegalStateException("No backend selected.");
        }
        // Configure DFS DSTL for this test as it might produce too much GC pressure if
        // ChangelogStateBackend is used.
        // Doing it on cluster level unconditionally as randomization currently happens on the job
        // level (environment); while this factory can only be set on the cluster level.
        FsStateChangelogStorageFactory.configure(
                config, tempFolder.newFolder(), Duration.ofMinutes(1), 10);

        return config;
    }

    private void setupRocksDB(
            Configuration config, int fileSizeThreshold, boolean incrementalCheckpoints)
            throws IOException {
        // Configure the managed memory size as 64MB per slot for rocksDB state backend.
        config.set(
                TaskManagerOptions.MANAGED_MEMORY_SIZE,
                MemorySize.ofMebiBytes(PARALLELISM / NUM_OF_TASK_MANAGERS * 64));

        final String rocksDb = tempFolder.newFolder().getAbsolutePath();
        final File backups = tempFolder.newFolder().getAbsoluteFile();
        // we use the fs backend with small threshold here to test the behaviour with file
        // references, not self contained byte handles
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incrementalCheckpoints);
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                Path.fromLocalFile(backups).toUri().toString());
        if (fileSizeThreshold != -1) {
            config.set(
                    CheckpointingOptions.FS_SMALL_FILE_THRESHOLD,
                    MemorySize.parse(fileSizeThreshold + "b"));
        }
        config.set(RocksDBOptions.LOCAL_DIRECTORIES, rocksDb);
    }

    private void setupForSt(Configuration config, int fileSizeThreshold) throws IOException {
        // Configure the managed memory size as 64MB per slot for rocksDB state backend.
        config.set(
                TaskManagerOptions.MANAGED_MEMORY_SIZE,
                MemorySize.ofMebiBytes(PARALLELISM / NUM_OF_TASK_MANAGERS * 64));

        final String forstdb = tempFolder.newFolder().getAbsolutePath();
        final File backups = tempFolder.newFolder().getAbsoluteFile();
        // we use the fs backend with small threshold here to test the behaviour with file
        // references, not self contained byte handles
        config.set(StateBackendOptions.STATE_BACKEND, "forst");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                Path.fromLocalFile(backups).toUri().toString());
        if (fileSizeThreshold != -1) {
            config.set(
                    CheckpointingOptions.FS_SMALL_FILE_THRESHOLD,
                    MemorySize.parse(fileSizeThreshold + "b"));
        }
        config.set(ForStOptions.LOCAL_DIRECTORIES, forstdb);
    }

    protected Configuration createClusterConfig() throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final File haDir = temporaryFolder.newFolder();

        Configuration config = new Configuration();
        config.set(RpcOptions.FRAMESIZE, String.valueOf(MAX_MEM_STATE_SIZE) + "b");

        if (zkServer != null) {
            config.set(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
            config.set(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
            config.set(HighAvailabilityOptions.HA_STORAGE_PATH, haDir.toURI().toString());
        }
        return config;
    }

    @Before
    public void setupTestCluster() throws Exception {
        configuration = getConfigurationSafe();
        miniClusterResource = getMiniClusterResource();
        miniClusterResource.before();
    }

    @After
    public void stopTestCluster() throws IOException {
        if (miniClusterResource != null) {
            miniClusterResource.after();
            miniClusterResource = null;
        }

        if (zkServer != null) {
            zkServer.close();
            zkServer = null;
        }

        // Prints a message when finishing a test method to avoid Travis' <tt>"Maven produced no
        // output
        // for xxx seconds."</tt> messages.
        System.out.println(
                "Finished " + getClass().getCanonicalName() + "#" + name.getMethodName() + ".");
    }

    // ------------------------------------------------------------------------

    @Test
    public void testTumblingTimeWindow() {
        final int numElementsPerKey = numElementsPerKey();
        final int windowSize = windowSize();
        final int numKeys = numKeys();

        try {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100);
            RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
            env.getConfig().setUseSnapshotCompression(true);

            env.addSource(
                            new FailingSource(
                                    new KeyedEventTimeGenerator(numKeys, windowSize),
                                    numElementsPerKey))
                    .rebalance()
                    .keyBy(x -> x.f0)
                    .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowSize)))
                    .apply(
                            new RichWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    Long,
                                    TimeWindow>() {

                                private boolean open = false;

                                @Override
                                public void open(OpenContext openContext) {
                                    assertEquals(
                                            PARALLELISM,
                                            getRuntimeContext()
                                                    .getTaskInfo()
                                                    .getNumberOfParallelSubtasks());
                                    open = true;
                                }

                                @Override
                                public void apply(
                                        Long l,
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> values,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out) {

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    int sum = 0;
                                    long key = -1;

                                    for (Tuple2<Long, IntType> value : values) {
                                        sum += value.f1.value;
                                        key = value.f0;
                                    }

                                    final Tuple4<Long, Long, Long, IntType> result =
                                            new Tuple4<>(
                                                    key,
                                                    window.getStart(),
                                                    window.getEnd(),
                                                    new IntType(sum));
                                    out.collect(result);
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new SinkValidatorUpdateFun(numElementsPerKey),
                                    new SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSize)))
                    .setParallelism(1);

            env.execute("Tumbling Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTumblingTimeWindowWithKVStateMinMaxParallelism() {
        doTestTumblingTimeWindowWithKVState(PARALLELISM);
    }

    @Test
    public void testTumblingTimeWindowWithKVStateMaxMaxParallelism() {
        doTestTumblingTimeWindowWithKVState(1 << 15);
    }

    public void doTestTumblingTimeWindowWithKVState(int maxParallelism) {
        final int numElementsPerKey = numElementsPerKey();
        final int windowSize = windowSize();
        final int numKeys = numKeys();

        try {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(PARALLELISM);
            env.setMaxParallelism(maxParallelism);
            env.enableCheckpointing(100);
            RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
            env.getConfig().setUseSnapshotCompression(true);

            env.addSource(
                            new FailingSource(
                                    new KeyedEventTimeGenerator(numKeys, windowSize),
                                    numElementsPerKey))
                    .rebalance()
                    .keyBy(x -> x.f0)
                    .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowSize)))
                    .apply(
                            new RichWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    Long,
                                    TimeWindow>() {

                                private boolean open = false;

                                private ValueState<Integer> count;

                                @Override
                                public void open(OpenContext openContext) {
                                    assertEquals(
                                            PARALLELISM,
                                            getRuntimeContext()
                                                    .getTaskInfo()
                                                    .getNumberOfParallelSubtasks());
                                    open = true;
                                    count =
                                            getRuntimeContext()
                                                    .getState(
                                                            new ValueStateDescriptor<>(
                                                                    "count", Integer.class, 0));
                                }

                                @Override
                                public void apply(
                                        Long l,
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> values,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out)
                                        throws Exception {

                                    // the window count state starts with the key, so that we get
                                    // different count results for each key
                                    if (count.value() == 0) {
                                        count.update(l.intValue());
                                    }

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    count.update(count.value() + 1);
                                    out.collect(
                                            new Tuple4<>(
                                                    l,
                                                    window.getStart(),
                                                    window.getEnd(),
                                                    new IntType(count.value())));
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new CountingSinkValidatorUpdateFun(),
                                    new SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSize)))
                    .setParallelism(1);

            env.execute("Tumbling Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSlidingTimeWindow() {
        final int numElementsPerKey = numElementsPerKey();
        final int windowSize = windowSize();
        final int windowSlide = windowSlide();
        final int numKeys = numKeys();

        try {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setMaxParallelism(2 * PARALLELISM);
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100);
            RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
            env.getConfig().setUseSnapshotCompression(true);

            env.addSource(
                            new FailingSource(
                                    new KeyedEventTimeGenerator(numKeys, windowSlide),
                                    numElementsPerKey))
                    .rebalance()
                    .keyBy(x -> x.f0)
                    .window(
                            SlidingEventTimeWindows.of(
                                    Duration.ofMillis(windowSize), Duration.ofMillis(windowSlide)))
                    .apply(
                            new RichWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    Long,
                                    TimeWindow>() {

                                private boolean open = false;

                                @Override
                                public void open(OpenContext openContext) {
                                    assertEquals(
                                            PARALLELISM,
                                            getRuntimeContext()
                                                    .getTaskInfo()
                                                    .getNumberOfParallelSubtasks());
                                    open = true;
                                }

                                @Override
                                public void apply(
                                        Long l,
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> values,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out) {

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    int sum = 0;
                                    long key = -1;

                                    for (Tuple2<Long, IntType> value : values) {
                                        sum += value.f1.value;
                                        key = value.f0;
                                    }
                                    final Tuple4<Long, Long, Long, IntType> output =
                                            new Tuple4<>(
                                                    key,
                                                    window.getStart(),
                                                    window.getEnd(),
                                                    new IntType(sum));
                                    out.collect(output);
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new SinkValidatorUpdateFun(numElementsPerKey),
                                    new SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSlide)))
                    .setParallelism(1);

            env.execute("Tumbling Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPreAggregatedTumblingTimeWindow() {
        final int numElementsPerKey = numElementsPerKey();
        final int windowSize = windowSize();
        final int numKeys = numKeys();

        try {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100);
            RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
            env.getConfig().setUseSnapshotCompression(true);

            env.addSource(
                            new FailingSource(
                                    new KeyedEventTimeGenerator(numKeys, windowSize),
                                    numElementsPerKey))
                    .rebalance()
                    .keyBy(x -> x.f0)
                    .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowSize)))
                    .reduce(
                            new ReduceFunction<Tuple2<Long, IntType>>() {

                                @Override
                                public Tuple2<Long, IntType> reduce(
                                        Tuple2<Long, IntType> a, Tuple2<Long, IntType> b) {
                                    return new Tuple2<>(a.f0, new IntType(a.f1.value + b.f1.value));
                                }
                            },
                            new RichWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    Long,
                                    TimeWindow>() {

                                private boolean open = false;

                                @Override
                                public void open(OpenContext openContext) {
                                    assertEquals(
                                            PARALLELISM,
                                            getRuntimeContext()
                                                    .getTaskInfo()
                                                    .getNumberOfParallelSubtasks());
                                    open = true;
                                }

                                @Override
                                public void apply(
                                        Long l,
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> input,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out) {

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    for (Tuple2<Long, IntType> in : input) {
                                        final Tuple4<Long, Long, Long, IntType> output =
                                                new Tuple4<>(
                                                        in.f0,
                                                        window.getStart(),
                                                        window.getEnd(),
                                                        in.f1);
                                        out.collect(output);
                                    }
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new SinkValidatorUpdateFun(numElementsPerKey),
                                    new SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSize)))
                    .setParallelism(1);

            env.execute("Tumbling Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPreAggregatedSlidingTimeWindow() {
        final int numElementsPerKey = numElementsPerKey();
        final int windowSize = windowSize();
        final int windowSlide = windowSlide();
        final int numKeys = numKeys();

        try {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100);
            RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
            env.getConfig().setUseSnapshotCompression(true);

            env.addSource(
                            new FailingSource(
                                    new KeyedEventTimeGenerator(numKeys, windowSlide),
                                    numElementsPerKey))
                    .rebalance()
                    .keyBy(x -> x.f0)
                    .window(
                            SlidingEventTimeWindows.of(
                                    Duration.ofMillis(windowSize), Duration.ofMillis(windowSlide)))
                    .reduce(
                            new ReduceFunction<Tuple2<Long, IntType>>() {

                                @Override
                                public Tuple2<Long, IntType> reduce(
                                        Tuple2<Long, IntType> a, Tuple2<Long, IntType> b) {

                                    // validate that the function has been opened properly
                                    return new Tuple2<>(a.f0, new IntType(a.f1.value + b.f1.value));
                                }
                            },
                            new RichWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    Long,
                                    TimeWindow>() {

                                private boolean open = false;

                                @Override
                                public void open(OpenContext openContext) {
                                    assertEquals(
                                            PARALLELISM,
                                            getRuntimeContext()
                                                    .getTaskInfo()
                                                    .getNumberOfParallelSubtasks());
                                    open = true;
                                }

                                @Override
                                public void apply(
                                        Long l,
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> input,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out) {

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    for (Tuple2<Long, IntType> in : input) {
                                        out.collect(
                                                new Tuple4<>(
                                                        in.f0,
                                                        window.getStart(),
                                                        window.getEnd(),
                                                        in.f1));
                                    }
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new SinkValidatorUpdateFun(numElementsPerKey),
                                    new SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSlide)))
                    .setParallelism(1);

            env.execute("Tumbling Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /** For validating the stateful window counts. */
    static class CountingSinkValidatorUpdateFun
            implements ValidatingSink.CountUpdater<Tuple4<Long, Long, Long, IntType>> {

        @Override
        public void updateCount(
                Tuple4<Long, Long, Long, IntType> value, Map<Long, Integer> windowCounts) {

            windowCounts.merge(value.f0, 1, (a, b) -> a + b);

            // verify the contents of that window, the contents should be:
            // (key + num windows so far)
            assertEquals(
                    "Window counts don't match for key " + value.f0 + ".",
                    value.f0.intValue() + windowCounts.get(value.f0),
                    value.f3.value);
        }
    }

    // ------------------------------------

    static class SinkValidatorUpdateFun
            implements ValidatingSink.CountUpdater<Tuple4<Long, Long, Long, IntType>> {

        private final int elementsPerKey;

        SinkValidatorUpdateFun(int elementsPerKey) {
            this.elementsPerKey = elementsPerKey;
        }

        @Override
        public void updateCount(
                Tuple4<Long, Long, Long, IntType> value, Map<Long, Integer> windowCounts) {
            // verify the contents of that window, Tuple4.f1 and .f2 are the window start/end
            // the sum should be "sum (start .. end-1)"

            int expectedSum = 0;
            // we shorten the range if it goes beyond elementsPerKey, because those are "incomplete"
            // sliding windows
            long countUntil = Math.min(value.f2, elementsPerKey);
            for (long i = value.f1; i < countUntil; i++) {
                // only sum up positive vals, to filter out the negative start of the
                // first sliding windows
                if (i > 0) {
                    expectedSum += i;
                }
            }

            assertEquals(
                    "Window start: " + value.f1 + " end: " + value.f2, expectedSum, value.f3.value);

            windowCounts.merge(value.f0, 1, (val, increment) -> val + increment);
        }
    }

    static class SinkValidatorCheckFun implements ValidatingSink.ResultChecker {

        private final int numKeys;
        private final int numWindowsExpected;

        SinkValidatorCheckFun(int numKeys, int elementsPerKey, int elementsPerWindow) {
            this.numKeys = numKeys;
            this.numWindowsExpected = elementsPerKey / elementsPerWindow;
        }

        @Override
        public boolean checkResult(Map<Long, Integer> windowCounts) {
            if (windowCounts.size() == numKeys) {
                for (Integer windowCount : windowCounts.values()) {
                    if (windowCount < numWindowsExpected) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
    }

    static class KeyedEventTimeGenerator implements FailingSource.EventEmittingGenerator {

        private final int keyUniverseSize;
        private final int watermarkTrailing;

        public KeyedEventTimeGenerator(int keyUniverseSize, int numElementsPerWindow) {
            this.keyUniverseSize = keyUniverseSize;
            // we let the watermark a bit behind, so that there can be in-flight timers that
            // required checkpointing
            // to include correct timer snapshots in our testing.
            this.watermarkTrailing = 4 * numElementsPerWindow / 3;
        }

        @Override
        public void emitEvent(
                SourceFunction.SourceContext<Tuple2<Long, IntType>> ctx, int eventSequenceNo) {
            final IntType intTypeNext = new IntType(eventSequenceNo);
            for (long i = 0; i < keyUniverseSize; i++) {
                final Tuple2<Long, IntType> generatedEvent = new Tuple2<>(i, intTypeNext);
                ctx.collectWithTimestamp(generatedEvent, eventSequenceNo);
            }

            ctx.emitWatermark(new Watermark(eventSequenceNo - watermarkTrailing));
        }
    }

    private int numElementsPerKey() {
        return 3000;
    }

    private int windowSize() {
        return 1000;
    }

    private int windowSlide() {
        return 100;
    }

    private int numKeys() {
        return 100;
    }
}
