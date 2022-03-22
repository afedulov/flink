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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Format.Projectable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.AbstractConfigurable;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.configurable.WithCharset;
import org.apache.flink.configuration.configurable.WithLenientParsing;
import org.apache.flink.connector.file.FileFormat;
import org.apache.flink.connector.file.FileFormat.BulkWritable;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.formats.common.Converter;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.Projection;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.lang3.StringEscapeUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class Csv<T, SELF extends Csv<T, SELF>> extends AbstractConfigurable<SELF>
        implements FileFormat<T, SELF>,
                BulkWritable<T>,
                Projectable<SELF>,
                WithCharset<SELF>,
                WithLenientParsing<SELF> {

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("field-delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Optional field delimiter character (',' by default)");

    public static final ConfigOption<Boolean> DISABLE_QUOTE_CHARACTER =
            ConfigOptions.key("disable-quote-character")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to disabled quote character for enclosing field values (false by default)\n"
                                    + "if true, quote-character can not be set");

    public static final ConfigOption<String> QUOTE_CHARACTER =
            ConfigOptions.key("quote-character")
                    .stringType()
                    .defaultValue("\"")
                    .withDescription(
                            "Optional quote character for enclosing field values ('\"' by default)");

    public static final ConfigOption<Boolean> ALLOW_COMMENTS =
            ConfigOptions.key("allow-comments")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to ignore comment lines that start with \"#\"\n"
                                    + "(disabled by default);\n"
                                    + "if enabled, make sure to also ignore parse errors to allow empty rows");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors");

    public static final ConfigOption<String> ARRAY_ELEMENT_DELIMITER =
            ConfigOptions.key("array-element-delimiter")
                    .stringType()
                    .defaultValue(";")
                    .withDescription(
                            "Optional array element delimiter string for separating\n"
                                    + "array and row element values (\";\" by default)");

    public static final ConfigOption<String> ESCAPE_CHARACTER =
            ConfigOptions.key("escape-character")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional escape character for escaping values (disabled by default)");

    public static final ConfigOption<String> NULL_LITERAL =
            ConfigOptions.key("null-literal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional null literal string that is interpreted as a\n"
                                    + "null value (disabled by default)");
    static final String IDENTIFIER = "csv";

    final CsvMapper mapper;
    final CsvSchema schema;
    private Projection projection;

    private Csv(CsvMapper mapper, CsvSchema schema, Configuration configuration) {
        super(configuration, Csv.class);
        this.mapper = checkNotNull(mapper);
        this.schema = checkNotNull(schema);
    }

    Csv() {
        super(Csv.class);
        mapper = null;
        schema = null;
    }

    public static <T> TypedCsv<T> forPojo(Class<T> pojoType) {
        CsvMapper mapper = new CsvMapper();
        return forSchema(mapper, mapper.schemaFor(pojoType))
                .withOutputType(TypeInformation.of(pojoType));
    }

    public static <T> UntypedCsv<T> forSchema(CsvMapper mapper, CsvSchema schema) {
        return new UntypedCsv<>(mapper, schema, new Configuration());
    }

    public <R extends T> TypedCsv<R> withOutputType(TypeInformation<R> typeInformation) {
        return new TypedCsv<>(mapper, schema, configuration, typeInformation);
    }

    @Override
    public SELF withProjection(int[][] projections) {
        this.projection = Projection.of(projections);
        return self();
    }

    public static <T> UntypedCsv<T> forSchema(CsvSchema schema) {
        return forSchema(new CsvMapper(), schema);
    }

    CsvSchema buildCsvSchema() {
        validateFormatOptions();

        final CsvSchema.Builder csvBuilder = schema.rebuild();
        // format properties
        configuration
                .getOptional(FIELD_DELIMITER)
                .map(s -> StringEscapeUtils.unescapeJava(s).charAt(0))
                .ifPresent(csvBuilder::setColumnSeparator);

        if (configuration.get(DISABLE_QUOTE_CHARACTER)) {
            csvBuilder.disableQuoteChar();
        } else {
            configuration
                    .getOptional(QUOTE_CHARACTER)
                    .map(s -> s.charAt(0))
                    .ifPresent(csvBuilder::setQuoteChar);
        }

        configuration.getOptional(ALLOW_COMMENTS).ifPresent(csvBuilder::setAllowComments);

        configuration
                .getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(csvBuilder::setArrayElementSeparator);

        configuration
                .getOptional(ESCAPE_CHARACTER)
                .map(s -> s.charAt(0))
                .ifPresent(csvBuilder::setEscapeChar);

        configuration.getOptional(NULL_LITERAL).ifPresent(csvBuilder::setNullValue);

        return csvBuilder.build();
    }

    void validateFormatOptions() {
        final boolean hasQuoteCharacter = configuration.getOptional(QUOTE_CHARACTER).isPresent();
        final boolean isDisabledQuoteCharacter = configuration.get(DISABLE_QUOTE_CHARACTER);
        if (isDisabledQuoteCharacter && hasQuoteCharacter) {
            throw new ValidationException(
                    "Format cannot define a quote character and disabled quote character at the same time.");
        }
        // Validate the option value must be a single char.
        validateCharacterVal(configuration, FIELD_DELIMITER, true);
        validateCharacterVal(configuration, ARRAY_ELEMENT_DELIMITER);
        validateCharacterVal(configuration, QUOTE_CHARACTER);
        validateCharacterVal(configuration, ESCAPE_CHARACTER);
    }

    /** Validates the option {@code option} value must be a Character. */
    private void validateCharacterVal(ReadableConfig tableOptions, ConfigOption<String> option) {
        validateCharacterVal(tableOptions, option, false);
    }

    /**
     * Validates the option {@code option} value must be a Character.
     *
     * @param tableOptions the table options
     * @param option the config option
     * @param unescape whether to unescape the option value
     */
    private void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option, boolean unescape) {
        if (!tableOptions.getOptional(option).isPresent()) {
            return;
        }

        final String value =
                unescape
                        ? StringEscapeUtils.unescapeJava(tableOptions.get(option))
                        : tableOptions.get(option);
        if (value.length() != 1) {
            throw new ValidationException(
                    String.format(
                            "Option '%s.%s' must be a string with single character, but was: %s",
                            IDENTIFIER, option.key(), tableOptions.get(option)));
        }
    }

    public BulkWriter.Factory<T> asWriter() {
        final Converter<T, T, Void> converter = (value, context) -> value;
        return new CsvBulkWriter.Factory<>(
                mapper, buildCsvSchema(), converter, null, configuration.get(CHARSET));
    }

    public static class UntypedCsv<T> extends Csv<T, UntypedCsv<T>> {

        private UntypedCsv(CsvMapper mapper, CsvSchema schema, Configuration configuration) {
            super(mapper, schema, configuration);
        }
    }

    public static class TypedCsv<T> extends Csv<T, TypedCsv<T>>
            implements StreamReadable<T, TypedCsv<T>> {

        private final TypeInformation<T> typeInformation;

        public TypedCsv(
                CsvMapper mapper,
                CsvSchema schema,
                Configuration configuration,
                TypeInformation<T> typeInformation) {
            super(mapper, schema, configuration);
            this.typeInformation = checkNotNull(typeInformation);
        }

        @Override
        public TypeInformation<T> getTypeInformation() {
            return typeInformation;
        }

        /** Builds a new {@code CsvReaderFormat} using a {@code CsvSchema}. */
        public StreamFormat<T> asStreamFormat() {
            return new CsvReaderFormat<>(
                    mapper,
                    schema,
                    typeInformation.getTypeClass(),
                    (value, context) -> value,
                    typeInformation,
                    configuration.get(CsvFormatOptions.IGNORE_PARSE_ERRORS),
                    configuration.get(CHARSET));
        }
    }
}
