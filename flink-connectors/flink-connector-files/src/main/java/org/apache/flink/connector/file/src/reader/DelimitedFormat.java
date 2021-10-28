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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Scanner;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A reader format that reads blocks of bytes from a file. The blocks are separated by the specified
 * delimiting pattern and deserialized using the provided deserialization schema.
 *
 * <p>The reader uses Java's built-in {@link Scanner} to decode the byte stream using various
 * supported charset encodings.
 *
 * <p>This format does not support optimized recovery from checkpoints. On recovery, it will re-read
 * and discard the number of lined that were processed before the last checkpoint. That is due to
 * the fact that the offsets of lines in the file cannot be tracked through the charset decoders
 * with their internal buffering of stream input and charset decoder state.
 */
@PublicEvolving
public class DelimitedFormat<T> extends SimpleStreamFormat<T> {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_CHARSET_NAME = "UTF-8";

    private final String charset;
    private final String delimiter;
    private final DeserializationSchema<T> deserializationSchema;

    private DelimitedFormat(
            String charset, String delimiter, DeserializationSchema<T> deserializationSchema) {
        this.charset = charset;
        this.delimiter = delimiter;
        this.deserializationSchema = deserializationSchema;
        checkNotNull(charset);
        checkNotNull(deserializationSchema);
    }

    /**
     * Static factory method for initializing {@code DelimitedFormat}. Uses UTF-8 charset by
     * default.
     *
     * @param delimiter The pattern used to split the file content into blocks of bytes.
     * @param deserializationSchema The schema used for converting the blocks of bytes into records
     *     of type {@code <T>}
     * @param <T> The type of the output elements.
     * @return
     */
    public static <T> DelimitedFormat<T> of(
            String delimiter, DeserializationSchema<T> deserializationSchema) {
        return new DelimitedFormat<>(DEFAULT_CHARSET_NAME, delimiter, deserializationSchema);
    }

    /**
     * Static factory method for initializing {@code DelimitedFormat}.
     *
     * @param delimiter The pattern used to split the file content into blocks of bytes.
     * @param charsetName The name of the charset used for decoding the bytes. Java's
     * @param deserializationSchema The schema used for converting the blocks of bytes into records
     *     of type {@code <T>}
     * @param <T> The type of the output elements.
     * @return
     */
    public static <T> DelimitedFormat<T> of(
            String delimiter, String charsetName, DeserializationSchema<T> deserializationSchema) {
        return new DelimitedFormat<>(charsetName, delimiter, deserializationSchema);
    }

    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        final Scanner scanner = new Scanner(stream);
        scanner.useDelimiter(delimiter);
        return new Reader(scanner);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // ------------------------------------------------------------------------

    /** The actual reader for the {@code DelimitedFormat}. */
    public final class Reader implements StreamFormat.Reader<T> {

        private final Scanner scanner;

        Reader(final Scanner scanner) {
            this.scanner = scanner;
        }

        @Nullable
        @Override
        public T read() throws IOException {
            byte[] token = scanner.hasNext() ? scanner.next().getBytes(charset) : null;
            return token != null ? deserializationSchema.deserialize(token) : null;
        }

        @Override
        public void close() throws IOException {
            scanner.close();
        }
    }
}
