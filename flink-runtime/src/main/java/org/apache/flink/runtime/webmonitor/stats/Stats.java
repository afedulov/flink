package org.apache.flink.runtime.webmonitor.stats;

/** Represents one or more statistics samples. */
public interface Stats {

    /**
     * Returns the timestamp when the last sample of this {@link Stats} was collected.
     *
     * @return the timestamp of the last sample.
     */
    long getEndTime();
}
