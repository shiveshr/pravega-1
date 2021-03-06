/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.ClientConfig;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.AssertExtensions;
import java.net.URI;
import javax.net.ssl.SSLHandshakeException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for TLS enabled standalone cluster. It inherits the test methods defined in the parent class.
 */
@Slf4j
public class TlsEnabledInProcPravegaClusterTest extends InProcPravegaClusterTest {

    @Before
    @Override
    public void setUp() throws Exception {
        this.authEnabled = false;
        this.tlsEnabled = true;
        super.setUp();
    }

    @Override
    String scopeName() {
        return "TlsTestScope";
    }

    @Override
    String streamName() {
        return "TlsTestStream";
    }

    @Override
    String eventMessage() {
        return "Test message on the encrypted channel";
    }

    @Override
    ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                .validateHostName(false)
                .build();
    }

    /**
     * This test verifies that create stream fails when the client config is invalid.
     *
     * Note: The timeout being used for the test is kept rather large so that there is ample time for the expected
     * exception to be raised even in case of abnormal delays in test environments.
     */
    @Test(timeout = 30000)
    public void testCreateStreamFailsWithInvalidClientConfig() {
        // Truststore for the TLS connection is missing.
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                .build();

        ControllerImplConfig controllerImplConfig = ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .retryAttempts(10)
                .initialBackoffMillis(1000)
                .backoffMultiple(1)
                .maxBackoffMillis(1000)
                .build();

        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(clientConfig, controllerImplConfig);

        AssertExtensions.assertThrows("TLS exception did not occur.",
                () -> streamManager.createScope(scopeName()),
                e -> hasTlsException(e));
    }

    private boolean hasTlsException(Throwable e) {
        return ExceptionUtils.indexOfThrowable(e, SSLHandshakeException.class) != -1;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
