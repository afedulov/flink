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

package org.apache.flink.connector.datagen.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * A stream generator function that returns a sequence of elements.
 *
 * <p>This generator function serializes the elements using Flink's type information. That way, any
 * object transport using Java serialization will not be affected by the serializability of the
 * elements.
 *
 * <p><b>NOTE:</b> This source has a parallelism of 1.
 *
 * @param <T> The type of elements returned by this function.
 */
@Experimental
public class FromElementsGeneratorFunction<T> implements GeneratorFunction<Long, T> {

    private static final long serialVersionUID = 1L;

    /** The (de)serializer to be used for the data elements. */
    private final TypeSerializer<T> serializer;

    /** The actual data elements, in serialized form. */
    private final byte[] elementsSerialized;

    /** The number of elements emitted already. */
    private int numElementsEmitted;

    private transient ByteArrayInputStream bais;
    private transient DataInputView input;

    public FromElementsGeneratorFunction(TypeSerializer<T> serializer, T... elements)
            throws IOException {
        this(serializer, Arrays.asList(elements));
    }

    public FromElementsGeneratorFunction(TypeSerializer<T> serializer, Iterable<T> elements)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);

        int count = 0;
        try {
            for (T element : elements) {
                serializer.serialize(element, wrapper);
                count++;
            }
        } catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }

        this.serializer = serializer;
        this.elementsSerialized = baos.toByteArray();
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        this.bais = new ByteArrayInputStream(elementsSerialized);
        this.input = new DataInputViewStreamWrapper(bais);
    }

    @Override
    public T map(Long nextIndex) throws Exception {
        // Move iterator to the required position in case of failure recovery
        while (numElementsEmitted < nextIndex) {
            numElementsEmitted++;
            tryDeserialize(serializer, input);
        }
        numElementsEmitted++;
        return tryDeserialize(serializer, input);
    }

    private T tryDeserialize(TypeSerializer<T> serializer, DataInputView input) throws IOException {
        try {
            return serializer.deserialize(input);
        } catch (Exception e) {
            throw new IOException(
                    "Failed to deserialize an element from the source. "
                            + "If you are using user-defined serialization (Value and Writable types), check the "
                            + "serialization functions.\nSerializer is "
                            + serializer,
                    e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Verifies that all elements in the collection are non-null, and are of the given class, or a
     * subclass thereof.
     *
     * @param elements The collection to check.
     * @param viewedAs The class to which the elements must be assignable to.
     * @param <OUT> The generic type of the collection to be checked.
     */
    public static <OUT> void checkCollection(Collection<OUT> elements, Class<OUT> viewedAs) {
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
}
