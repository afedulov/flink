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

package org.apache.flink.api.connector.source.lib.util;

import java.io.Serializable;

/** The interface to rate limit execution of methods. */
interface RateLimiter extends Serializable {

    /**
     * Acquire method is a blocking call that is intended to be used in places where it is required
     * to limit the rate at which results are produced or other functions are called.
     *
     * @return The number of milliseconds this call blocked its caller.
     * @throws InterruptedException The interrupted exception.
     */
    int acquire() throws InterruptedException;
}
