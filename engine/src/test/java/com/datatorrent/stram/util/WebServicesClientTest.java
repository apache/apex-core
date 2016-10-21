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
package com.datatorrent.stram.util;

import java.lang.reflect.Field;

import org.junit.Assert;
import org.junit.Test;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;

import com.datatorrent.stram.security.AuthScheme;

/**
 *
 */
public class WebServicesClientTest
{
  private static final String CREDENTIALS_PROVIDER_FIELD = "credentialsProvider";

  @Test
  public void testFilterPresent()
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    HeaderClientFilter clientFilter = new HeaderClientFilter();
    webServicesClient.addFilter(clientFilter);
    Assert.assertTrue("Filter present", webServicesClient.isFilterPresent(clientFilter));
  }

  public static void checkUserCredentials(String username, String password, AuthScheme authScheme) throws NoSuchFieldException,
      IllegalAccessException
  {
    CredentialsProvider provider = getCredentialsProvider();
    String httpScheme = AuthScope.ANY_SCHEME;
    if (authScheme == AuthScheme.BASIC) {
      httpScheme = AuthSchemes.BASIC;
    } else if (authScheme == AuthScheme.DIGEST) {
      httpScheme = AuthSchemes.DIGEST;
    }
    AuthScope authScope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, httpScheme);
    Credentials credentials = provider.getCredentials(authScope);
    Assert.assertNotNull("Credentials", credentials);
    Assert.assertTrue("Credentials type is user", UsernamePasswordCredentials.class.isAssignableFrom(credentials.getClass()));
    UsernamePasswordCredentials pwdCredentials = (UsernamePasswordCredentials)credentials;
    Assert.assertEquals("Username", username, pwdCredentials.getUserName());
    Assert.assertEquals("Password", password, pwdCredentials.getPassword());
  }

  private static CredentialsProvider getCredentialsProvider() throws NoSuchFieldException, IllegalAccessException
  {
    Field field = WebServicesClient.class.getDeclaredField(CREDENTIALS_PROVIDER_FIELD);
    field.setAccessible(true);
    CredentialsProvider credentials = (CredentialsProvider)field.get(null);
    field.setAccessible(false);
    return credentials;
  }

}
