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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import java.io.Serializable;
import java.util.List;

/**
 * Back pressure statistics of multiple tasks.
 *
 * <p>Statistics are gathered by sampling stack traces of running tasks. The
 * back pressure ratio denotes the ratio of traces indicating back pressure
 * to the total number of sampled traces.
 */
public class OperatorFlameGraph implements Serializable, Stats {

	/**
	 * Graph node.
	 */
	public static class Node {

		private final String name;
		private final int value;
		private final List<Node> children;

		Node(String name, int value, List<Node> children) {
			this.name = name;
			this.value = value;
			this.children = children;
		}

		public String getName() {
			return name;
		}

		public int getValue() {
			return value;
		}

		public List<Node> getChildren() {
			return children;
		}
	}

	private static final long serialVersionUID = 1L;

	/** End time stamp of the corresponding sample. */
	private final long endTimestamp;

	private final Node root;

	public OperatorFlameGraph(long endTimestamp, Node root) {
		this.endTimestamp = endTimestamp;
		this.root = root;
	}

	public long getEndTimestamp() {
		return endTimestamp;
	}

	public Node getRoot() {
		return root;
	}

	//TODO: REMOVE/IMPLEMENT
	//	@Override
//	public String toString() {
//		return "OperatorBackPressureStats{" +
//				"sampleId=" + sampleId +
//				", endTimestamp=" + endTimestamp +
//				", subTaskBackPressureRatio=" + Arrays.toString(subTaskBackPressureRatio) +
//				'}';
//	}
}
