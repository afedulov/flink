package org.apache.flink.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.slf4j.helpers.BasicMDCAdapter;
import org.slf4j.spi.MDCAdapter;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class MdcLogbackCompatibilityTest {

    private MDCAdapter originalAdapter;

    @BeforeEach
    void setUp() throws Exception {
        originalAdapter = getCurrentMDCAdapter();
        setMDCAdapter(new BasicMDCAdapter());
    }

    @AfterEach
    void tearDown() throws Exception {
        setMDCAdapter(originalAdapter);
    }

    /*
     * Logback 1.2 setContextMap() method does not accept nulls
     * see https://issues.apache.org/jira/browse/FLINK-36227 for details
     */
    @Test
    void testContextRestorationWorksWithNullContext() {
        assertThat(MDC.getCopyOfContextMap()).isNull();

        MdcUtils.MdcCloseable restoreContext = MdcUtils.withContext(Collections.singletonMap("k", "v"));
        assertThat(MDC.get("k")).isEqualTo("v");
        assertDoesNotThrow(restoreContext::close);
        assertThat(MDC.get("k")).isNull();
    }

    private MDCAdapter getCurrentMDCAdapter() throws Exception {
        Field adapterField = MDC.class.getDeclaredField("mdcAdapter");
        adapterField.setAccessible(true);
        return (MDCAdapter) adapterField.get(null);
    }

    private void setMDCAdapter(MDCAdapter adapter) throws Exception {
        Field adapterField = MDC.class.getDeclaredField("mdcAdapter");
        adapterField.setAccessible(true);
        adapterField.set(null, adapter);
    }
}
