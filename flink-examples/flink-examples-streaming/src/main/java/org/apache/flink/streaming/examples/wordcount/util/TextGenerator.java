package org.apache.flink.streaming.examples.wordcount.util;

import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;

public class TextGenerator extends ParallelBaseGenerator<String> {
    public TextGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    protected String randomEvent(SplittableRandom rnd, long id) {
        return WordCountData.WORDS[ThreadLocalRandom.current().nextInt(WordCountData.WORDS.length)];
    }
}
