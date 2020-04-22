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
package org.apache.kafka.common.network.banzaicloud;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.banzaicloud.AuthenticationContextWithOptionalAuthId;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;

/**
 *
 */
public class PlaintextChannelWithOptionalUserIdentity implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(PlaintextChannelWithOptionalUserIdentity.class);
    private final ListenerName listenerName;
    private Map<String, ?> configs;
    private SslPrincipalMapper sslPrincipalMapper;

    public PlaintextChannelWithOptionalUserIdentity(ListenerName listenerName) {
        this.listenerName = listenerName;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
        String sslPrincipalMappingRules = (String) configs.get(BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG);
        if (sslPrincipalMappingRules != null)
            sslPrincipalMapper = SslPrincipalMapper.fromRules(sslPrincipalMappingRules);
    }

    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException {
        try {
            ReadBufferedPlaintextTransportLayer transportLayer = new ReadBufferedPlaintextTransportLayer(key);
            Supplier<Authenticator> authenticatorCreator = () -> new PrefixLookupAuthenticator(configs, transportLayer, listenerName, sslPrincipalMapper);
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                    memoryPool != null ? memoryPool : MemoryPool.NONE);
        } catch (Exception e) {
            log.warn("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    /**
     * A basic authenticator implementation which differs from
     * {@link org.apache.kafka.common.network.PlaintextChannelBuilder.PlaintextAuthenticator}
     * in only a few parts. It implements {@link PrefixLookupAuthenticator ::authenticate} as a pre-processor of
     * data received right after a connection was established with broker.
     * The {@link PrefixLookupAuthenticator ::complete} method indicates the above mentioned process' state.
     *
     */
    private static class PrefixLookupAuthenticator implements Authenticator {
        private final static Logger log = LoggerFactory.getLogger(PrefixLookupAuthenticator.class);
        private final static String BZC_PREFIX = "BZC_UID:";
        private final static byte BZC_PKG_SIZE = 2;

        private final ReadBufferedPlaintextTransportLayer transportLayer;
        private final KafkaPrincipalBuilder principalBuilder;
        private final ListenerName listenerName;


        private volatile String authorizationId;
        private ByteBuffer buffer;
        private boolean limiterOk;
        private int limiter;

        public PrefixLookupAuthenticator(Map<String, ?> configs, ReadBufferedPlaintextTransportLayer transportLayer, ListenerName listenerName, SslPrincipalMapper sslPrincipalMapper) {
            this.transportLayer = transportLayer;
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, transportLayer, this, null, sslPrincipalMapper);
            this.listenerName = listenerName;
        }

        /**
         * piggyback on the transport layer and handle the 1st message as an authorization id
         * @throws IOException - network layer errors
         */
        @Override
        synchronized public void authenticate() throws IOException {
            if (authorizationId != null) {
                log.debug("Already authenticated");
                return;
            }

            if (limiterOk) {
                log.debug("Authenticating: phase[2/2]");
                buffer = ByteBuffer.allocate(limiter);
                log.debug("Buffer allocated for payload with size: " + limiter);
                transportLayer.read(buffer);
                buffer.flip();
                buffer.position(BZC_PKG_SIZE);
                byte[] payload = new byte[buffer.remaining()];
                buffer.get(payload, 0, buffer.remaining());
                String text = new String(payload, StandardCharsets.UTF_8);
                log.debug("Received payload message: " + text);

                if (hasBZCPrefix(text)) {
                    log.debug("BZC prefix: present");
                    authorizationId = extractAuthId(text);
                    transportLayer.ignoreExtraPayloadFrom(limiter);
                } else {
                    log.debug("BZC prefix: not present");
                    authorizationId = KafkaPrincipal.ANONYMOUS.getName();
                }
                transportLayer.authenticationDone();
            } else {
                log.debug("Authenticating: phase[1/2]");
                if (buffer == null) {
                    log.debug("Buffer allocated with size: " + BZC_PKG_SIZE);
                    buffer = ByteBuffer.allocate(BZC_PKG_SIZE);
                }

                log.debug("Reading network to get payload length");
                transportLayer.read(buffer);

                buffer.flip();
                log.trace("buffer {}", new ReadBufferedPlaintextTransportLayer.BufferDetails(buffer));

                if (buffer.remaining() > 0) {
                    limiter = buffer.getShort(0) + BZC_PKG_SIZE;
                    limiterOk = true;
                    log.debug("Payload length present, len: " + limiter);
                    transportLayer.gotPayloadLength();
                } else {
                    log.debug("Payload length not present, went with anonymous");
                    authorizationId = KafkaPrincipal.ANONYMOUS.getName();
                    transportLayer.authenticationDone();
                }
            }
            buffer = null;
        }

        @Override
        public KafkaPrincipal principal() {
            InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();

            if (listenerName == null) {
                throw new IllegalStateException("Unexpected call to principal() when listenerName is null");
            }
            if (authorizationId == null) {
                throw new IllegalStateException("Unexpected call to principal() when authorizationId is null");
            }

            KafkaPrincipal kp = principalBuilder.build(new AuthenticationContextWithOptionalAuthId(
                    clientAddress,
                    listenerName.value(),
                    authorizationId
            ));
            log.debug("KafkaPrincipal: " + kp);
            return kp;
        }

        @Override
        public boolean complete() {
            if (authorizationId == null && transportLayer.isConnected()) {
                try {
                    authenticate();
                } catch (IOException e) {
                    return false;
                }
            }
            return authorizationId != null;
        }

        @Override
        public void close() {}

        private String extractAuthId(String str) {
            String[] result = str.split(BZC_PREFIX);
            if (result.length == 2) {
                log.debug("Extracted BZC postfix: " + result[1]);
                return result[1];
            }

            log.warn(BZC_PREFIX + " prefixed id missing, went with anonymous");
            return KafkaPrincipal.ANONYMOUS.getName();
        }

        private boolean hasBZCPrefix(String str) {
            return str.startsWith(BZC_PREFIX);
        }
    }
}
