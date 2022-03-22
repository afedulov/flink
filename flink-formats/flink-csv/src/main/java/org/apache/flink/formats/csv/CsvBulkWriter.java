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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.common.Converter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A simple {@link BulkWriter} implementation based on Jackson CSV transformations. */
class CsvBulkWriter<T, R, C> implements BulkWriter<T> {

    private final FSDataOutputStream stream;
    private final Converter<T, R, C> converter;
    @Nullable private final C converterContext;
    private final ObjectWriter csvWriter;
    private final String charset;
    private final OutputStreamWriter writer;

    CsvBulkWriter(
            CsvMapper mapper,
            CsvSchema schema,
            Converter<T, R, C> converter,
            @Nullable C converterContext,
            FSDataOutputStream stream,
            String charset) {

        this.stream = checkNotNull(stream);
        this.converter = checkNotNull(converter);
        this.converterContext = converterContext;
        this.csvWriter = mapper.writer(checkNotNull(schema));
        checkArgument(Charset.isSupported(charset), "Unknown charset");
        this.charset = checkNotNull(charset);
        this.writer = new OutputStreamWriter(stream, Charset.forName(charset));

        // Prevent Jackson's writeValue() method calls from closing the stream.
        mapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    }

    @Override
    public void addElement(T element) throws IOException {
        final R r = converter.convert(element, converterContext);
        csvWriter.writeValue(writer, r);
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void finish() throws IOException {
        writer.flush();
        stream.sync();
    }

    static class Factory<T> implements BulkWriter.Factory<T> {
        private final CsvMapper mapper;
        private final CsvSchema schema;
        private final Converter<T, Object, Object> converter;
        private final Object converterContext;
        private final String charset;

        <R, C> Factory(
                CsvMapper mapper,
                CsvSchema schema,
                Converter<T, R, C> converter,
                C converterContext,
                String charset) {
            this.mapper = mapper;
            this.schema = schema;
            this.converter = (Converter<T, Object, Object>) converter;
            this.converterContext = converterContext;
            this.charset = charset;
        }

        @Override
        public BulkWriter<T> create(FSDataOutputStream out) throws IOException {
            return new CsvBulkWriter<>(mapper, schema, converter, converterContext, out, charset);
        }
    }
}
