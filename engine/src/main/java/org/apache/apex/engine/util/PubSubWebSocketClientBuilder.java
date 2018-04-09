/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.engine.util;

import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.util.PubSubWebSocketClient;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;

/**
 * @since 3.7.0
 */
public class PubSubWebSocketClientBuilder
{
  public static final String GATEWAY_LOGIN_URL_PATH = "/ws/v2/login";

  private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketClientBuilder.class);

  private Context context;

  public PubSubWebSocketClientBuilder setContext(Context context)
  {
    this.context = context;
    return this;
  }

  private <T extends PubSubWebSocketClient> T build(Class<T> clazz)
  {
    Preconditions.checkState(context != null, "Context not specified");
    String gatewayAddress = context.getValue(LogicalPlan.GATEWAY_CONNECT_ADDRESS);
    if (gatewayAddress != null) {
      int timeout = context.getValue(LogicalPlan.PUBSUB_CONNECT_TIMEOUT_MILLIS);
      boolean gatewayUseSsl = context.getValue(LogicalPlan.GATEWAY_USE_SSL);

      // The builder can be used to build different types of PubSub clients in future but for now only one is supported
      SharedPubSubWebSocketClient wsClient = null;

      try {
        wsClient = new SharedPubSubWebSocketClient((gatewayUseSsl ? "wss://" : "ws://") + gatewayAddress + "/pubsub", timeout);

        String gatewayUserName = context.getValue(LogicalPlan.GATEWAY_USER_NAME);
        String gatewayPassword = context.getValue(LogicalPlan.GATEWAY_PASSWORD);
        if (gatewayUserName != null && gatewayPassword != null) {
          wsClient.setLoginUrl((gatewayUseSsl ? "https://" : "http://") + gatewayAddress + GATEWAY_LOGIN_URL_PATH);
          wsClient.setUserName(gatewayUserName);
          wsClient.setPassword(gatewayPassword);
        }

        return (T)wsClient;
      } catch (URISyntaxException e) {
        logger.warn("Unable to initialize websocket for gateway address {}", gatewayAddress, e);
      }

      return null;
    }

    return null;
  }

  public SharedPubSubWebSocketClient build()
  {
    return build(SharedPubSubWebSocketClient.class);
  }

}
