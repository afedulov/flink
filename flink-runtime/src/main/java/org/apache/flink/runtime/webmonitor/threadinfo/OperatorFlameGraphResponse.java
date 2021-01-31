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

package org.apache.flink.runtime.webmonitor.threadinfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** Wrapper for {@link OperatorFlameGraph}. */
public class OperatorFlameGraphResponse implements Serializable {

    public static OperatorFlameGraphResponse of(@Nullable OperatorFlameGraph operatorFlameGraph) {
        return new OperatorFlameGraphResponse(operatorFlameGraph);
    }

    private static final long serialVersionUID = 1L;

    @Nullable private final OperatorFlameGraph operatorFlameGraph;

    private OperatorFlameGraphResponse(@Nullable final OperatorFlameGraph operatorFlameGraph) {
        this.operatorFlameGraph = operatorFlameGraph;
    }

    public Optional<OperatorFlameGraph> getOperatorFlameGraph() {
        return Optional.ofNullable(operatorFlameGraph);
    }
}
