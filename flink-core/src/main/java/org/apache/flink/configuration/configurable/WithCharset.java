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

package org.apache.flink.configuration.configurable;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configurable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A {@link Configurable} that allows users setting a {@link Charset}.
 */
public interface WithCharset<SELF extends Configurable<SELF>> extends Configurable<SELF> {

    ConfigOption<String> CHARSET =
            ConfigOptions.key("charset")
                    .stringType()
                    .defaultValue(StandardCharsets.UTF_8.displayName())
                    .withDescription("Defines the string charset.");

    /** Sets the charset and returns this. */
    default SELF withCharset(Charset charset) {
        return withCharset(charset.name());
    }

    /** Sets the charset and returns this. */
    default SELF withCharset(String charset) {
        return withOption(CHARSET, charset);
    }
}
