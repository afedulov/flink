/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.util.SerializedValue;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

/** An {@link OperatorEventGateway} for testing purpose. */
public class MockOperatorEventGateway implements OperatorEventGateway {

    private final LinkedList<OperatorEvent> events;

    public MockOperatorEventGateway() {
        events = new LinkedList<>();
    }

    @Override
    public void sendEventToCoordinator(OperatorEvent event) {
        events.add(event);
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            SerializedValue<CoordinationRequest> request) {
        return null;
    }

    public OperatorEvent getNextEvent() {
        return events.removeFirst();
    }
}
