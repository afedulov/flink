package org.apache.flink.connector.file;

import org.apache.flink.api.common.serialization.BulkWriter.Factory;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.Format;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface FileFormat<T, SELF extends FileFormat<T, SELF>> extends Format<T, SELF> {

    /**
     * Trait of {@link Format} that signals that the format can be used in a {@link FileSink}.
     *
     * <p>This trait should be mostly used by implementors of a {@link FileFormat}, so that the
     * format can be converted into a {@link Factory}.
     *
     * <p>This trait should not be used in conjunction with {@link StreamWritable}.
     */
    interface BulkWritable<T> {

        Factory<T> asWriter();
    }

    /**
     * Trait of {@link Format} that signals that the format can be used in a {@link FileSink}.
     *
     * <p>This trait should be mostly used by implementors of a {@link FileFormat}, so that the
     * format can be converted into a {@link Encoder}.
     *
     * <p>This trait should not be used in conjunction with {@link BulkWritable}.
     */
    interface StreamWritable<T> {

        Encoder<T> asEncoder();
    }

    /**
     * Trait of {@link Format} that signals that the format can be used in a {@link FileSource}.
     *
     * <p>This trait should be mostly used by implementors of a {@link FileFormat}, so that the
     * format can be converted into a {@link StreamFormat}.
     *
     * <p>This trait should not be used in conjunction with {@link BulkReadable}.
     */
    interface StreamReadable<T, SELF extends StreamReadable<T, SELF>>
            extends WithTypeInformation<T>, FileFormat<T, SELF> {

        StreamFormat<T> asStreamFormat();
    }

    /**
     * Trait of {@link Format} that signals that the format can be used in a {@link FileSource}.
     *
     * <p>This trait should be mostly used by implementors of a {@link FileFormat}, so that the
     * format can be converted into a {@link BulkFormat}.
     *
     * <p>This trait should not be used in conjunction with {@link StreamReadable}.
     */
    interface BulkReadable<T, SELF extends BulkReadable<T, SELF>>
            extends WithTypeInformation<T>, FileFormat<T, SELF> {

        <SplitT extends FileSourceSplit> BulkFormat<T, SplitT> asBulkFormat();
    }

    default Set<ConfigOption<?>> forwardOptions() {
        return Stream.concat(requiredOptions().stream(), optionalOptions().stream())
                .collect(Collectors.toSet());
    }
}
