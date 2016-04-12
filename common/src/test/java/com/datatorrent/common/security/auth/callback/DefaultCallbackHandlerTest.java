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
import java.util.Collection;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Attribute;

/**
 * Test for default callback handler
 */
public class DefaultCallbackHandlerTest
{
  @Test
  public void testHandler()
  {
    DefaultCallbackHandler handler = new DefaultCallbackHandler();
    SecurityContext context = new SecurityContext();
    handler.setup(context);
    Callback[] callbacks = new Callback[3];
    callbacks[0] = new NameCallback("UserName:");
    callbacks[1] = new PasswordCallback("Password:", false);
    callbacks[2] = new RealmCallback("Realm:");
    try {
      handler.handle(callbacks);
      Assert.assertEquals("Username", "user1", ((NameCallback)callbacks[0]).getName());
      Assert.assertEquals("Password", "pass", new String(((PasswordCallback)callbacks[1]).getPassword()));
      Assert.assertEquals("Realm", "default", ((RealmCallback)callbacks[2]).getText());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } catch (UnsupportedCallbackException e) {
      Assert.fail(e.getMessage());
    }
  }

  private static class SecurityContext implements com.datatorrent.common.security.SecurityContext
  {
    static {
      Attribute.AttributeMap.AttributeInitializer.initialize(com.datatorrent.common.security.SecurityContext.class);
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(Attribute<T> key)
    {
      if (key.equals(SecurityContext.USER_NAME)) {
        return (T)"user1";
      } else if (key.equals(SecurityContext.PASSWORD)) {
        return (T)("pass".toCharArray());
      } else if (key.equals(SecurityContext.REALM)) {
        return (T)"default";
      }
      return null;
    }

    @Override
    public void setCounters(Object counters)
    {
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
    }
  }

}
