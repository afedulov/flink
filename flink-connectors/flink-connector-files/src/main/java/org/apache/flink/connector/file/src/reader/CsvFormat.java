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

package org.apache.flink.connector.file.src.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.formats.common.ConverterRegistry;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;


@PublicEvolving
public class CsvFormat extends SimpleStreamFormat<Row> {

    private static final long serialVersionUID = 1L;

    private final String charsetName;
    private final Charset charset;
    private final CsvRowDeserializationSchema deserializationSchema;

    //TODO: adding CsvRowDeserializationSchema introduced a circular dependency.
    public CsvFormat(String charsetName, CsvRowDeserializationSchema deserializationSchema) {
        this.charsetName = charsetName;
        this.charset = Charset.forName(charsetName);
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(stream, charsetName));
        return new Reader(reader);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // ------------------------------------------------------------------------

    public final class Reader implements StreamFormat.Reader<Row> {

        private final BufferedReader reader;

        Reader(final BufferedReader reader) {
            this.reader = reader;
        }

        @Nullable
        @Override
        public Row read() throws IOException {
            byte[] csvLine = reader.readLine().getBytes(charset);
            return deserializationSchema.deserialize(csvLine);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
