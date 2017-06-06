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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.util.FSUtil;

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

  public static long refreshTokens(long tokenLifeTime, String destinationDir, String destinationFile, final Configuration conf, String principal, String hdfsKeyTabFile, final Credentials credentials, final InetSocketAddress rmAddress, final boolean renewRMToken) throws IOException
  {
    long expiryTime = System.currentTimeMillis() + tokenLifeTime;
    //renew tokens
    final String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
    }

    File keyTabFile;
    try (FileSystem fs = FileSystem.newInstance(conf)) {
      keyTabFile = FSUtil.copyToLocalFileSystem(fs, destinationDir, destinationFile, hdfsKeyTabFile, conf);
    }

    if (principal == null) {
      principal = UserGroupInformation.getCurrentUser().getUserName();
    }
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTabFile.getAbsolutePath());
    try {
      ugi.doAs(new PrivilegedExceptionAction<Object>()
      {
        @Override
        public Object run() throws Exception
        {

          Credentials creds = new Credentials();
          try (FileSystem fs1 = FileSystem.newInstance(conf)) {
            fs1.addDelegationTokens(tokenRenewer, creds);
          }
          if (renewRMToken) {
            try (YarnClient yarnClient = StramClientUtils.createYarnClient(conf)) {
              new StramClientUtils.ClientRMHelper(yarnClient, conf).addRMDelegationToken(tokenRenewer, creds);
            }
          }
          credentials.addAll(creds);

          return null;
        }
      });
      UserGroupInformation.getCurrentUser().addCredentials(credentials);
    } catch (InterruptedException e) {
      LOG.error("Error while renewing tokens ", e);
      expiryTime = System.currentTimeMillis();
    } catch (IOException e) {
      LOG.error("Error while renewing tokens ", e);
      expiryTime = System.currentTimeMillis();
    }
    LOG.debug("number of tokens: {}", credentials.getAllTokens().size());
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.debug("updated token: {}", token);
    }
    keyTabFile.delete();
    return expiryTime;
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
