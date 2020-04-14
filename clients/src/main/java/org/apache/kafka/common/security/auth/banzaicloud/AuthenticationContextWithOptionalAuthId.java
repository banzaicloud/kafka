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
package org.apache.kafka.common.security.auth.banzaicloud;

import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;

import java.net.InetAddress;

/**
 * To bypass authorization controllers which are not aware of banzai
 * the class must be derived from {@link PlaintextAuthenticationContext}
 */
public class AuthenticationContextWithOptionalAuthId extends PlaintextAuthenticationContext {
    private final String authorizationId;

    public AuthenticationContextWithOptionalAuthId(InetAddress clientAddress, String listenerName, String authorizationId) {
        super(clientAddress, listenerName);
        this.authorizationId = authorizationId;
    }

    public String getAuthorizationId() {
        return authorizationId;
    }
}

