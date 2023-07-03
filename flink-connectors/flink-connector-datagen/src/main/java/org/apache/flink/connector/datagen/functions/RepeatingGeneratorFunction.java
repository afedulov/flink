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

package org.apache.flink.connector.datagen.functions;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A stream generator function that returns a sequence of elements.
 *
 * <p>This generator function serializes the elements using Flink's type information. That way, any
 * object transport using Java serialization will not be affected by the serializability of the
 * elements.
 *
 * @param <OUT> The type of elements returned by this function.
 */
@Experimental
public class RepeatingGeneratorFunction<OUT> implements GeneratorFunction<Long, OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RepeatingGeneratorFunction.class);

    /** The (de)serializer to be used for the data elements. */
    private final TypeSerializer<OUT> serializer;

    /** The actual data elements, in serialized form. */
    private byte[] elementsSerialized;

    /** The number of elements emitted already. */
    private int numElementsEmitted;

    private transient DataInputView input;

    @SafeVarargs
    public RepeatingGeneratorFunction(TypeInformation<OUT> typeInfo, OUT... elements) {
        this(typeInfo, new ExecutionConfig(), Arrays.asList(elements));
    }

    public RepeatingGeneratorFunction(TypeInformation<OUT> typeInfo, Iterable<OUT> elements) {
        this(typeInfo, new ExecutionConfig(), elements);
    }

    public RepeatingGeneratorFunction(
            TypeInformation<OUT> typeInfo, ExecutionConfig config, Iterable<OUT> elements) {
        // must not have null elements and mixed elements
        checkIterable(elements, typeInfo.getTypeClass());
        this.serializer = typeInfo.createSerializer(config);
        trySerialize(elements);
    }

    public RepeatingGeneratorFunction(TypeInformation<OUT> typeInfo, List<OUT> elements) {
        this(typeInfo, new ExecutionConfig(), elements);
    }

    private void serializeElements(Iterable<OUT> elements) throws IOException {
        Preconditions.checkState(serializer != null, "serializer not set");
        LOG.info("Serializing elements using  " + serializer);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);

        try {
            for (OUT element : elements) {
                serializer.serialize(element, wrapper);
            }
        } catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }
        this.elementsSerialized = baos.toByteArray();
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        InputStream inputStream = new CircularInputStream(elementsSerialized);
        this.input = new DataInputViewStreamWrapper(inputStream);
    }

    @Override
    public OUT map(Long nextIndex) throws Exception {
        // Move iterator to the required position in case of failure recovery
        while (numElementsEmitted < nextIndex) {
            numElementsEmitted++;
            tryDeserialize(serializer, input);
        }
        OUT result = tryDeserialize(serializer, input);
        System.out.println(String.format("Map: %s -> %s", nextIndex, result));
        numElementsEmitted++;

        return result;
    }

    private OUT tryDeserialize(TypeSerializer<OUT> serializer, DataInputView input)
            throws IOException {
        try {
            return serializer.deserialize(input);
        } catch (EOFException eof) {
            throw new NoSuchElementException(
                    "Reached the end of the collection. This could be caused by issues with the serializer or by calling the map() function more times than there are elements in the collection. Make sure that you set the number of records to be produced by the DataGeneratorSource equal to the number of elements in the collection.");
        } catch (Exception e) {
            throw new IOException(
                    "Failed to deserialize an element from the source. "
                            + "If you are using user-defined serialization (Value and Writable types), check the "
                            + "serialization functions.\nSerializer is "
                            + serializer,
                    e);
        }
    }

    private void trySerialize(Iterable<OUT> elements) {
        try {
            serializeElements(elements);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Verifies that all elements in the iterable are non-null, and are of the given class, or a
     * subclass thereof.
     *
     * @param elements The iterable to check.
     * @param viewedAs The class to which the elements must be assignable to.
     * @param <OUT> The generic type of the iterable to be checked.
     */
    public static <OUT> void checkIterable(Iterable<OUT> elements, Class<?> viewedAs) {
        for (OUT elem : elements) {
            if (elem == null) {
                throw new IllegalArgumentException("The collection contains a null element");
            }

            if (!viewedAs.isAssignableFrom(elem.getClass())) {
                throw new IllegalArgumentException(
                        "The elements in the collection are not all subclasses of "
                                + viewedAs.getCanonicalName());
            }
        }
    }

    private static class CircularInputStream extends InputStream {
        private final byte[] buffer;
        private final AtomicInteger index;

        public CircularInputStream(byte[] buffer) {
            this.buffer = buffer;
            this.index = new AtomicInteger(0);
        }

        @Override
        public int read() throws IOException {
            if (buffer.length == 0) {
                throw new IOException("Empty buffer");
            }

            int result = buffer[index.getAndIncrement() % buffer.length];

            // Convert signed byte to unsigned.
            if (result < 0) {
                result += 256;
            }

            return result;
        }
    }
}
