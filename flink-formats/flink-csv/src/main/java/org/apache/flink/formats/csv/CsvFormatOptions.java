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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

/** Options for CSV format. */
@PublicEvolving
@Deprecated
public class CsvFormatOptions {

    public static final ConfigOption<String> FIELD_DELIMITER = Csv.FIELD_DELIMITER;

    public static final ConfigOption<Boolean> DISABLE_QUOTE_CHARACTER = Csv.DISABLE_QUOTE_CHARACTER;

    public static final ConfigOption<String> QUOTE_CHARACTER = Csv.QUOTE_CHARACTER;

    public static final ConfigOption<Boolean> ALLOW_COMMENTS = Csv.ALLOW_COMMENTS;

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = Csv.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> ARRAY_ELEMENT_DELIMITER = Csv.ARRAY_ELEMENT_DELIMITER;

    public static final ConfigOption<String> ESCAPE_CHARACTER = Csv.ESCAPE_CHARACTER;

    public static final ConfigOption<String> NULL_LITERAL = Csv.NULL_LITERAL;

    private CsvFormatOptions() {}
}
