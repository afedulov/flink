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

package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.api.connector.source.datagen.DataGeneratorSource;

import java.util.Optional;

/** A simple example of generating data with {@link DataGeneratorSource}. */
public class Main {

    public static void main(String[] args) throws Exception {
        Optional<Integer> opt1 = Optional.ofNullable(null);
        Optional<Integer> opt2 = Optional.empty();

        System.out.println(opt1);
        System.out.println(opt2);

        Integer integer1 = opt1.orElse(null);
        Integer integer2 = opt2.orElse(null);
        System.out.println(integer1);
        System.out.println(integer2);

        //        opt1.flatMap();
    }
}
