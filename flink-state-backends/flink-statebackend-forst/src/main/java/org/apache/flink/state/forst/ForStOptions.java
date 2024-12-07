/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.state.forst.ForStStateBackend.PriorityQueueStateType.ForStDB;

/** Configuration options for the ForStStateBackend. */
@Experimental
public class ForStOptions {

    /** The local directory (on the TaskManager) where ForSt puts some meta files. */
    public static final ConfigOption<String> LOCAL_DIRECTORIES =
            ConfigOptions.key("state.backend.forst.local-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The local directory (on the TaskManager) where ForSt puts some meta files. Per default, it will be <WORKING_DIR>/tmp. See %s for more details.",
                                            TextElement.code(
                                                    ClusterOptions
                                                            .TASK_MANAGER_PROCESS_WORKING_DIR_BASE
                                                            .key()))
                                    .build());

    /** The remote directory where ForSt puts its SST files. */
    public static final ConfigOption<String> REMOTE_DIRECTORY =
            ConfigOptions.key("state.backend.forst.remote-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The remote directory where ForSt puts its SST files, fallback to %s if not configured.",
                                    LOCAL_DIRECTORIES.key()));

    public static final ConfigOption<String> CACHE_DIRECTORY =
            ConfigOptions.key("state.backend.forst.cache.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The directory where ForSt caches its SST files, fallback to %s/cache if not configured.",
                                    LOCAL_DIRECTORIES.key()));

    public static final ConfigOption<Long> CACHE_SIZE_BASE_LIMIT =
            ConfigOptions.key("state.backend.forst.cache.size-based-limit")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The size-based capacity limit of cache, a non-positive number indicates that there is no limit.");

    public static final ConfigOption<Long> CACHE_RESERVED_SIZE =
            ConfigOptions.key("state.backend.forst.cache.reserve-size")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The reserved size of cache, when set to a positive number, dynamic space checking will be leveraged. "
                                    + "This option and the "
                                    + CACHE_SIZE_BASE_LIMIT.key()
                                    + "option can be set simultaneously, the smaller cache limit will be used as the upper limit.");

    /** The options factory class for ForSt to create DBOptions and ColumnFamilyOptions. */
    public static final ConfigOption<String> OPTIONS_FACTORY =
            ConfigOptions.key("state.backend.forst.options-factory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The options factory class for users to add customized options in DBOptions and ColumnFamilyOptions for ForSt. "
                                    + "If set, the ForSt state backend will load the class and apply configs to DBOptions and ColumnFamilyOptions "
                                    + "after loading ones from 'ForStConfigurableOptions' and pre-defined options.");

    public static final ConfigOption<Boolean> USE_MANAGED_MEMORY =
            ConfigOptions.key("state.backend.forst.memory.managed")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If set, the ForSt state backend will automatically configure itself to use the "
                                    + "managed memory budget of the task slot, and divide the memory over write buffers, indexes, "
                                    + "block caches, etc. That way, the three major uses of memory of ForSt will be capped.");

    public static final ConfigOption<MemorySize> FIX_PER_SLOT_MEMORY_SIZE =
            ConfigOptions.key("state.backend.forst.memory.fixed-per-slot")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The fixed total amount of memory, shared among all ForSt instances per slot. "
                                            + "This option overrides the '%s' option when configured.",
                                    USE_MANAGED_MEMORY.key()));

    public static final ConfigOption<MemorySize> FIX_PER_TM_MEMORY_SIZE =
            ConfigOptions.key("state.backend.forst.memory.fixed-per-tm")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The fixed total amount of memory, shared among all ForSt instances per Task Manager (cluster-level option). "
                                            + "This option only takes effect if neither '%s' nor '%s' are not configured. If none is configured "
                                            + "then each ForSt column family state has its own memory caches (as controlled by the column "
                                            + "family options). "
                                            + "The relevant options for the shared resources (e.g. write-buffer-ratio) can be set on the same level (config.yaml)."
                                            + "Note, that this feature breaks resource isolation between the slots",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    public static final ConfigOption<Double> WRITE_BUFFER_RATIO =
            ConfigOptions.key("state.backend.forst.memory.write-buffer-ratio")
                    .doubleType()
                    .defaultValue(0.5)
                    .withDescription(
                            String.format(
                                    "The maximum amount of memory that write buffers may take, as a fraction of the total shared memory. "
                                            + "This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    public static final ConfigOption<Double> HIGH_PRIORITY_POOL_RATIO =
            ConfigOptions.key("state.backend.forst.memory.high-prio-pool-ratio")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription(
                            String.format(
                                    "The fraction of cache memory that is reserved for high-priority data like index, filter, and "
                                            + "compression dictionary blocks. This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    public static final ConfigOption<Boolean> USE_PARTITIONED_INDEX_FILTERS =
            ConfigOptions.key("state.backend.forst.memory.partitioned-index-filters")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            String.format(
                                    "With partitioning, the index/filter block of an SST file is partitioned into smaller blocks with "
                                            + "an additional top-level index on them. When reading an index/filter, only top-level index is loaded into memory. "
                                            + "The partitioned index/filter then uses the top-level index to load on demand into the block cache "
                                            + "the partitions that are required to perform the index/filter query. "
                                            + "This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    /** Choice of timer service implementation. */
    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<ForStStateBackend.PriorityQueueStateType>
            TIMER_SERVICE_FACTORY =
                    ConfigOptions.key("state.backend.forst.timer-service.factory")
                            .enumType(ForStStateBackend.PriorityQueueStateType.class)
                            .defaultValue(ForStDB)
                            .withDescription(
                                    "This determines the factory for timer service state implementation.");

    /** The cache size per key-group for ROCKSDB timer service factory implementation. */
    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<Integer> FORST_TIMER_SERVICE_FACTORY_CACHE_SIZE =
            ConfigOptions.key("state.backend.forst.timer-service.cache-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            String.format(
                                    "The cache size per keyGroup of rocksdb timer service factory. This option only has an effect "
                                            + "when '%s' is configured to '%s'. Increasing this value can improve the performance "
                                            + "of rocksdb timer service, but consumes more heap memory at the same time.",
                                    TIMER_SERVICE_FACTORY.key(), ForStDB.name()));

    public static final ConfigOption<Boolean> EXECUTOR_COORDINATOR_INLINE =
            ConfigOptions.key("state.backend.forst.executor.inline-coordinator")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to let the task thread be the coordinator thread responsible for distributing requests.");

    public static final ConfigOption<Boolean> EXECUTOR_WRITE_IO_INLINE =
            ConfigOptions.key("state.backend.forst.executor.inline-write")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to let write requests executed within the coordinator thread.");

    public static final ConfigOption<Integer> EXECUTOR_READ_IO_PARALLELISM =
            ConfigOptions.key("state.backend.forst.executor.read-io-parallelism")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The number of threads used for read IO operations in the executor.");

    public static final ConfigOption<Integer> EXECUTOR_WRITE_IO_PARALLELISM =
            ConfigOptions.key("state.backend.forst.executor.write-io-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of threads used for write IO operations in the executor."
                                    + " Only valid when '"
                                    + EXECUTOR_WRITE_IO_INLINE.key()
                                    + "' is false.");
}
