/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.BanzaiAuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;

/**
 *
 */
public class BanzaiChannelBuilder implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(PlaintextChannelBuilder.class);
    private final ListenerName listenerName;
    private Map<String, ?> configs;

    public BanzaiChannelBuilder(ListenerName listenerName) {
        this.listenerName = listenerName;
    }

    @Override
    public void close() {
        log.info("close");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
        log.info("configs: " + configs.toString());
    }

    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException {
        try {
            PlaintextTransportLayer transportLayer = new PlaintextTransportLayer(key);
            Supplier<Authenticator> authenticatorCreator = () -> new BanzaiChannelBuilder.BanzaiAuthenticator(id, maxReceiveSize, configs, transportLayer, listenerName);
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                    memoryPool != null ? memoryPool : MemoryPool.NONE);
        } catch (Exception e) {
            log.warn("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    /**
     *
     */
    private static class BanzaiAuthenticator implements Authenticator {
        private final static Logger log = LoggerFactory.getLogger(BanzaiAuthenticator.class);

        private final PlaintextTransportLayer transportLayer;
        private final KafkaPrincipalBuilder principalBuilder;
        private final ListenerName listenerName;

        private final int maxReceiveSize;
        private final String connectionId;
        private NetworkReceive netInBuffer;
        private String authorizationId;

        public BanzaiAuthenticator(String connectionId, int maxReceiveSize, Map<String, ?> configs, PlaintextTransportLayer transportLayer, ListenerName listenerName) {
            this.connectionId = connectionId;
            this.maxReceiveSize = maxReceiveSize;
            this.transportLayer = transportLayer;
            this.principalBuilder = context -> {
                KafkaPrincipal principal = KafkaPrincipal.ANONYMOUS;

                if (context instanceof BanzaiAuthenticationContext) {
                    BanzaiAuthenticationContext ctx = (BanzaiAuthenticationContext) context;
                    principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, ctx.getAuthorizationId());
                }

                log.info("built principal: " + principal.toString());
                return principal;
            };
            this.listenerName = listenerName;
        }

        /**
         * piggyback on the transport layer and handle the 1st message as an authorization id
         * @throws AuthenticationException
         * @throws IOException
         */
        @Override
        public void authenticate() throws AuthenticationException, IOException {
            if (complete()) {
                throw new AuthenticationException("Unexpected call to authenticate() when authentication already completed!");
            }

            if (netInBuffer == null)
                netInBuffer = new NetworkReceive(maxReceiveSize, connectionId);

            netInBuffer.readFrom(transportLayer);
            if (!netInBuffer.complete())
                return;
            netInBuffer.payload().rewind();

            byte[] authorizationId = new byte[netInBuffer.payload().remaining()];
            netInBuffer.payload().get(authorizationId, 0, authorizationId.length);
            netInBuffer = null;
            this.authorizationId = new String(authorizationId, StandardCharsets.UTF_8);
        }

        @Override
        public KafkaPrincipal principal() {
            InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();

            if (listenerName == null)
                throw new IllegalStateException("Unexpected call to principal() when listenerName is null");
            if (!complete())
                throw new IllegalStateException("Unexpected call to principal() when authorizationId is null");

            return principalBuilder.build(new BanzaiAuthenticationContext(
                    clientAddress,
                    listenerName.value(),
                    authorizationId
            ));
        }

        @Override
        public boolean complete() {
            return authorizationId != null;
        }

        @Override
        public void close() throws IOException {
            if (netInBuffer != null) {
                netInBuffer.close();
                netInBuffer = null;
            }
        }
    }
}
