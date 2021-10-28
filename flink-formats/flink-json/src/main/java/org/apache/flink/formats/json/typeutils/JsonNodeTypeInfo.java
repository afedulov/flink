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

package org.apache.flink.formats.json.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class JsonNodeTypeInfo extends TypeInformation<JsonNode> {

    private static final long serialVersionUID = 4141977586453820650L;

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<JsonNode> getTypeClass() {
        return JsonNode.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    //  ----- Remove below
    @Override
    public TypeSerializer<JsonNode> createSerializer(ExecutionConfig config) {
        return null;

        // TODO: create JsonSerializer
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }

    //  ----- Remove above

    /*    @Override
    public TypeSerializer<JsonNode> createSerializer(ExecutionConfig config) {
        return new AvroSerializer<>(JsonNode.class, schema);
    }

    @Override
    public String toString() {
        return String.format("GenericRecord(\"%s\")", schema.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof JsonNodeTypeInfo) {
            JsonNodeTypeInfo avroTypeInfo = (JsonNodeTypeInfo) obj;
            return Objects.equals(avroTypeInfo.schema, schema);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schema);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof JsonNodeTypeInfo;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeUTF(schema.toString());
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        this.schema = new Schema.Parser().parse(ois.readUTF());
    }*/
}
