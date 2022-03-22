package org.apache.flink.configuration;

import java.util.Collections;
import java.util.Set;

/**
 * Base interface for everything that can be configured with {@link ConfigOption}s.
 *
 * @param <SELF> the specific implementing class for using the {@code withX} methods. Users can
 *     simply use a wildcard {@code Configurable<?>} after full configuration.
 */
public interface Configurable<SELF extends Configurable<SELF>> {

    <T> SELF withOption(ConfigOption<T> option, T value);

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory requires in
     * addition to {@link #optionalOptions()}.
     */
    default Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in
     * addition to {@link #requiredOptions()}.
     */
    default Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
