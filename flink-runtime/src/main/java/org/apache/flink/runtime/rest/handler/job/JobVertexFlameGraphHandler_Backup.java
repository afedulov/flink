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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorFlameGraph;
import org.apache.flink.runtime.rest.messages.*;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Request handler for the job vertex back pressure.
 */
public class JobVertexFlameGraphHandler_Backup extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, JobVertexFlameGraphInfo, JobVertexMessageParameters> {

	private static JobVertexFlameGraphInfo createJobVertexFlameGraphInfo(OperatorFlameGraph flameGraph) {
		return new JobVertexFlameGraphInfo(flameGraph.getEndTimestamp(), flameGraph.getRoot());
	}

	public JobVertexFlameGraphHandler_Backup(
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<EmptyRequestBody, JobVertexFlameGraphInfo, JobVertexMessageParameters> messageHeaders) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
	}

	@Override
	protected CompletableFuture<JobVertexFlameGraphInfo> handleRequest(
			@Nonnull HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			@Nonnull RestfulGateway gateway) throws RestHandlerException {
		final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
		final JobVertexID jobVertexId = request.getPathParameter(JobVertexIdPathParameter.class);
		return gateway
			.requestOperatorFlameGraph(jobId, jobVertexId)
			.thenApply(
				flameGraphStats -> flameGraphStats.getOperatorFlameGraph()
					.map(JobVertexFlameGraphHandler_Backup::createJobVertexFlameGraphInfo)
					.orElse(JobVertexFlameGraphInfo.empty()));
	}
}
