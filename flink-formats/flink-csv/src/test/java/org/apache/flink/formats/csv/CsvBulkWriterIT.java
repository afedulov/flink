package org.apache.flink.formats.csv;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.TestDataGenerators;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvBulkWriterIT {
    
    @TempDir
    File outDir;

    @Test
    public void testNoDataIsWrittenBeforeFlush() throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        
        // Workaround serialization limitations
        File outDirRef = new File(outDir.getAbsolutePath());

        FileSink<Pojo> sink = FileSink
                .forBulkFormat(
                        new org.apache.flink.core.fs.Path(outDir.getAbsolutePath()),
                        out -> new CsvBulkWriterWrapper<>(CsvBulkWriter.forPojo(Pojo.class, out), outDirRef
                                ))
                .build();

        List<Pojo> integers = Arrays.asList(new Pojo(1),new Pojo(2));
        DataGeneratorSource<Pojo> generatorSource = TestDataGenerators.fromDataWithSnapshotsLatch(
                integers,
                TypeInformation.of(Pojo.class));
        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "")
                .sinkTo(sink);
        env.execute();
        assertThat(getResultsFromSinkFiles(outDir)).containsSequence("1","2","1","2");
    }

    private static class CsvBulkWriterWrapper<T> implements BulkWriter<T> {
        
        private static int callCounter = 0;

        private final CsvBulkWriter<T, ?, ?> csvBulkWriter;
        
        private final File outDir;

        CsvBulkWriterWrapper(CsvBulkWriter<T, ?, ?> csvBulkWriter,
                             File outDir) {
            this.csvBulkWriter = csvBulkWriter;
            this.outDir = outDir;
        }

        @Override
        public void addElement(T element) throws IOException {
            callCounter++;
            System.out.println("--" + callCounter);
            csvBulkWriter.addElement(element);
            System.out.println(getFileContentByPath(outDir).keySet());
            System.out.println(getResultsFromSinkFiles(outDir));
            
            if(callCounter<3){
                 assertThat(getResultsFromSinkFiles(outDir)).isEmpty();
            }
            if(callCounter==3){
                 assertThat(getResultsFromSinkFiles(outDir)).containsSequence("1","2");
            }
        }

        @Override
        public void flush() throws IOException {
            csvBulkWriter.flush();
        }

        @Override
        public void finish() throws IOException {
            csvBulkWriter.finish();
        }
    }

    public static class Pojo {
        public long x;

        public Pojo(long x) {
            this.x = x;
        }

        public Pojo() {
        }
    }

    private static List<String> getResultsFromSinkFiles(File outDir) throws IOException {
        final Map<File, String> contents = getFileContentByPath(outDir);
        return   contents.entrySet().stream()
                        .flatMap(e -> Arrays.stream(e.getValue().split("\n")))
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
    }

    private static Map<File, String> getFileContentByPath(File directory) throws IOException {
        Map<File, String> contents = new HashMap<>();

        final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
        for (File file : filesInBucket) {
            contents.put(file, FileUtils.readFileToString(file));
        }
        return contents;
    }
}
