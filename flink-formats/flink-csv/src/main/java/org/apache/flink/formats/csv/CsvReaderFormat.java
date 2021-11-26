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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.formats.common.Converter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code StreamFormat} for reading CSV files.
 *
 * @param <T> The type of the returned elements.
 */
@PublicEvolving
public class CsvReaderFormat<T> extends SimpleStreamFormat<T> {

    private static final long serialVersionUID = 1L;

    private final CsvMapper mapper;
    private final CsvSchema schema;
    private final Class<Object> rootType;
    private final Converter<Object, T, Void> converter;
    private final TypeInformation<T> typeInformation;

    @SuppressWarnings("unchecked")
    <R> CsvReaderFormat(
            CsvMapper mapper,
            CsvSchema schema,
            Class<R> rootType,
            Converter<R, T, Void> converter,
            TypeInformation<T> typeInformation) {
        this.mapper = checkNotNull(mapper);
        this.schema = schema;
        this.rootType = (Class<Object>) checkNotNull(rootType);
        this.typeInformation = checkNotNull(typeInformation);
        this.converter = (Converter<Object, T, Void>) checkNotNull(converter);
    }

    /**
     * Builds a new {@code CsvFormat} using a {@code CsvSchema}.
     *
     * @param schema The Jackson CSV schema configured for parsing specific CSV files.
     * @param typeInformation The Flink type descriptor of the returned elements.
     * @param <T> The type of the returned elements.
     */
    public static <T> CsvReaderFormat<T> forSchema(
            CsvSchema schema, TypeInformation<T> typeInformation) {
        return forSchema(new CsvMapper(), schema, typeInformation);
    }

    /**
     * Builds a new {@code CsvFormat} using a {@code CsvSchema} and a pre-created {@code CsvMapper}.
     *
     * @param mapper The pre-created {@code CsvMapper}.
     * @param schema The Jackson CSV schema configured for parsing specific CSV files.
     * @param typeInformation The Flink type descriptor of the returned elements.
     * @param <T> The type of the returned elements.
     */
    public static <T> CsvReaderFormat<T> forSchema(
            CsvMapper mapper, CsvSchema schema, TypeInformation<T> typeInformation) {
        return new CsvReaderFormat<>(
                mapper,
                schema,
                typeInformation.getTypeClass(),
                (value, context) -> value,
                typeInformation);
    }

    /**
     * Builds a new {@code CsvFormat} for reading CSV files mapped to the provided POJO class
     * definition.
     *
     * @param pojoType The type class of the POJO.
     * @param <T> The type of the returned elements.
     */
    public static <T> CsvReaderFormat<T> forPojo(Class<T> pojoType) {
        CsvMapper mapper = new CsvMapper();
        return forSchema(mapper, mapper.schemaFor(pojoType), TypeInformation.of(pojoType));
    }

    @Override
    public StreamFormat.Reader<T> createReader(Configuration config, FSDataInputStream stream)
            throws IOException {
        return new Reader<>(mapper.readerFor(rootType).with(schema).readValues(stream), converter);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInformation;
    }

    // ------------------------------------------------------------------------

    /** The actual reader for the {@code CsvFormat}. */
    private static final class Reader<R, T> implements StreamFormat.Reader<T> {
        private final MappingIterator<R> iterator;
        private final Converter<R, T, Void> converter;

        public Reader(MappingIterator<R> iterator, Converter<R, T, Void> converter) {
            this.iterator = checkNotNull(iterator);
            this.converter = checkNotNull(converter);
        }

        @Nullable
        @Override
        public T read() throws IOException {
            return iterator.hasNext() ? converter.convert(iterator.next(), null) : null;
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }
    }
}
