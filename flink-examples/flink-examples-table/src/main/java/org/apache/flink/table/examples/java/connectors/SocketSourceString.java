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

package org.apache.flink.table.examples.java.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.examples.java.connectors.SocketSourceString.DummyCheckpoint;
import org.apache.flink.table.examples.java.connectors.SocketSourceString.DummySplit;
import org.apache.flink.util.InstantiationUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link SocketSourceString} opens a socket and consumes bytes.
 *
 * <p>It splits records by the given byte delimiter (`\n` by default) and delegates the decoding to
 * a pluggable {@link DeserializationSchema}.
 *
 * <p>Note: This is only an example and should not be used in production. The source function is not
 * fault-tolerant and can only work with a parallelism of 1.
 */
public final class SocketSourceString
        implements Source<String, DummySplit, DummyCheckpoint>, ResultTypeQueryable<String> {

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;
    private final DeserializationSchema<String> deserializer;

    private volatile boolean isRunning = true;
    private Socket currentSocket;

    public SocketSourceString(
            String hostname,
            int port,
            byte byteDelimiter,
            DeserializationSchema<String> deserializer) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<DummySplit, DummyCheckpoint> createEnumerator(
            SplitEnumeratorContext<DummySplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<DummySplit, DummyCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<DummySplit> enumContext, DummyCheckpoint checkpoint)
            throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<DummySplit> getSplitSerializer() {
        return new DummySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DummyCheckpoint> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<String, DummySplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new SocketReader();
    }

    public static class DummySplit implements SourceSplit {

        public static final DummySplit INSTANCE = new DummySplit();

        @Override
        public String splitId() {
            return "dummy";
        }
    }

    public static class DummyCheckpoint {}

    public class SocketReader implements SourceReader<String, DummySplit> {

        private Socket socket;
        private ByteArrayOutputStream buffer;
        private InputStream stream;
        int b;

        @Override
        public void start() {
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(hostname, port), 0);
                buffer = new ByteArrayOutputStream();
                stream = socket.getInputStream();
            } catch (Throwable t) {
                t.printStackTrace(); // print and continue
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
            while ((b = stream.read()) >= 0) {
                // buffer until delimiter
                if (b != byteDelimiter) {
                    buffer.write(b);
                }
                // decode and emit record
                else {
                    output.collect(deserializer.deserialize(buffer.toByteArray()));
                    buffer.reset();
                    return InputStatus.MORE_AVAILABLE;
                }
            }
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public List<DummySplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return null;
        }

        @Override
        public void addSplits(List<DummySplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() throws Exception {
            try {
                socket.close();
            } catch (Throwable t) {
                // ignore socket close exception, try to close the stream
            } finally {
                try {
                    stream.close();
                } catch (Throwable t) {
                    // ignore input stream close exception, try to close the buffer
                } finally {
                    buffer.close();
                }
            }
        }
    }

    public static class DummySplitSerializer implements SimpleVersionedSerializer<DummySplit> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(DummySplit split) throws IOException {
            return InstantiationUtil.serializeObject(split);
        }

        @Override
        public DummySplit deserialize(int version, byte[] serialized) throws IOException {
            try {
                return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize the split.", e);
            }
        }
    }
}
