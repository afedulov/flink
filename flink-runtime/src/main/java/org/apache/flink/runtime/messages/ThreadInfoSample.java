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

package org.apache.flink.runtime.messages;

import com.sun.istack.Nullable;

import java.io.Serializable;
import java.lang.management.ThreadInfo;

/**
 * A serializable wrapper container for transferring parts of the {@link java.lang.management.ThreadInfo}.
 */
public class ThreadInfoSample implements Serializable {

    private final Thread.State threadState;
    private final StackTraceElement[] stackTrace;

    private ThreadInfoSample(
            Thread.State threadState,
            StackTraceElement[] stackTrace) {
        this.threadState = threadState;
        this.stackTrace = stackTrace;
    }

    /**
     * Constructs a {@link ThreadInfoSample} from {@link ThreadInfo}.
     *
     * @param threadInfo {@link ThreadInfo} where the data will be copied from.
     * @return new {@link ThreadInfoSample}
     */
    public static @Nullable ThreadInfoSample from(@Nullable ThreadInfo threadInfo) {
        if (threadInfo != null) {
            return new ThreadInfoSample(
                    threadInfo.getThreadState(),
                    threadInfo.getStackTrace());
        } else {
            return null;
        }
    }

    public Thread.State getThreadState() {
        return threadState;
    }

    public StackTraceElement[] getStackTrace() {
        return stackTrace.clone();
    }

}
