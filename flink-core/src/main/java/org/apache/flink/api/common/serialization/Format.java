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

package org.apache.flink.api.common.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configurable;

import java.util.Locale;

/**
 * This class represents a data format that can be used for reading and writing data.
 *
 * <p>The format can be configured with {@link org.apache.flink.configuration.ConfigOption}s or
 * respective {@code withX} setters.
 *
 * @param <T> the data type
 * @param <SELF> the specific class type implementing this interface
 */
public interface Format<T, SELF extends Format<T, SELF>> extends Configurable<SELF> {

    default String getName() {
        return getClass().getSimpleName().toLowerCase(Locale.US);
    }

    /**
     * Trait of {@link Format} that signals that the format can be used in various places to
     * deserialize {@code byte[]} into objects.
     *
     * <p>This trait should be mostly used by implementors of a {@link Format}, so that the format
     * can be converted into a {@link DeserializationSchema}.
     */
    interface ByteDeserializable<T, SELF extends Format<T, SELF>> extends Format<T, SELF> {

        DeserializationSchema<T> asDeserializationSchema();
    }

    /**
     * Trait of {@link Format} that signals that the format can be used in various places to
     * serialize objects into {@code byte[]}.
     *
     * <p>This trait should be mostly used by implementors of a {@link Format}, so that the format
     * can be converted into a {@link SerializationSchema}.
     */
    interface ByteSerializable<T, SELF extends Format<T, SELF>> extends Format<T, SELF> {

        SerializationSchema<T> asSerializationSchema();
    }

    /**
     * A trait that shows that the format has explicit type information.
     *
     * <p>This trait should be mostly used by implementors of a {@link Format} and needs to be
     * present such that the format can be used in as a source.
     */
    interface WithTypeInformation<T> {

        TypeInformation<T> getTypeInformation();
    }
}
