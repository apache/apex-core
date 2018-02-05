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
package com.datatorrent.stram.security;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.datatorrent.api.StreamingApplication;

/**
 * <p>StramUserLogin class.</p>
 *
 * @since 0.3.2
 */
public class StramUserLogin
{
  private static final Logger LOG = LoggerFactory.getLogger(StramUserLogin.class);
  public static final String DT_AUTH_PREFIX = StreamingApplication.DT_PREFIX + "authentication.";
  public static final String DT_AUTH_PRINCIPAL = DT_AUTH_PREFIX + "principal";
  public static final String DT_AUTH_KEYTAB = DT_AUTH_PREFIX + "keytab";
  public static final String DT_APP_PATH_IMPERSONATED = DT_AUTH_PREFIX + "impersonation.path.enable";
  private static String principal;
  private static String keytab;

  public static void attemptAuthentication(Configuration conf) throws IOException
  {
    if (UserGroupInformation.isSecurityEnabled()) {
      authenticate(conf);
    }
  }

  public static void authenticate(Configuration conf) throws IOException
  {
    String userPrincipal = conf.get(DT_AUTH_PRINCIPAL);
    String userKeytab = conf.get(DT_AUTH_KEYTAB);
    authenticate(userPrincipal, userKeytab);
  }

  public static void authenticate(String principal, String keytab) throws IOException
  {
    if ((principal != null) && !principal.isEmpty()
        && (keytab != null) && !keytab.isEmpty()) {
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        LOG.info("Login user {}", UserGroupInformation.getCurrentUser().getUserName());
        StramUserLogin.principal = principal;
        StramUserLogin.keytab = keytab;
      } catch (IOException ie) {
        LOG.error("Error login user with principal {}", principal, ie);
        throw ie;
      }
    }
  }

  public static String getPrincipal()
  {
    return principal;
  }

  public static String getKeytab()
  {
    return keytab;
  }

}
