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
package com.datatorrent.common.security.auth.callback;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Component;
import com.datatorrent.common.security.SecurityContext;

/**
 * The default callback handler handles common callbacks and can be used with most common
 * JAAS login modules. This handler can be extended to create custom callback handlers if
 * needed. When developing a custom handler in this fashion most of time you would need to
 * just override the processCallback method, handle the custom callback and delegate to the
 * base class for the common callbacks.
 *
 * This handler implements Component to interface to receive the login information via the
 * SecurityContext. Custom handlers being implemented from scratch without extending this
 * handler can also extend Component to receive security information.
 *
 * @since 2.0.0
 */
public class DefaultCallbackHandler implements CallbackHandler, Component<SecurityContext>
{
  private static final Logger logger = LoggerFactory.getLogger(DefaultCallbackHandler.class);

  protected SecurityContext context;

  @Override
  public void setup(SecurityContext context)
  {
    this.context = context;
  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
  {
    for (Callback callback : callbacks) {
      processCallback(callback);
    }
  }

  protected void processCallback(Callback callback) throws IOException, UnsupportedCallbackException
  {
    if (callback instanceof NameCallback) {
      NameCallback namecb = (NameCallback)callback;
      namecb.setName(context.getValue(SecurityContext.USER_NAME));
    } else if (callback instanceof PasswordCallback) {
      PasswordCallback passcb = (PasswordCallback)callback;
      passcb.setPassword(context.getValue(SecurityContext.PASSWORD));
    } else if (callback instanceof RealmCallback) {
      RealmCallback realmcb = (RealmCallback)callback;
      realmcb.setText(context.getValue(SecurityContext.REALM));
    } else if (callback instanceof TextOutputCallback) {
      TextOutputCallback textcb = (TextOutputCallback)callback;
      if (textcb.getMessageType() == TextOutputCallback.INFORMATION) {
        logger.info(textcb.getMessage());
      } else if (textcb.getMessageType() == TextOutputCallback.WARNING) {
        logger.warn(textcb.getMessage());
      } else if (textcb.getMessageType() == TextOutputCallback.ERROR) {
        logger.error(textcb.getMessage());
      } else {
        logger.debug("Auth message type {}, message {}", textcb.getMessageType(), textcb.getMessage());
      }
    } else {
      throw new UnsupportedCallbackException(callback);
    }
  }
}
