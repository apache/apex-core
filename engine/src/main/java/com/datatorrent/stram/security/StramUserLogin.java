/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.security;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final String DT_AUTH_PRINCIPAL = DT_AUTH_PREFIX + "principal";
  public static final String DT_AUTH_KEYTAB = DT_AUTH_PREFIX + "keytab";
  private static String principal;
  private static String keytab;

  public static void attemptAuthentication(Configuration conf) throws IOException
  {
    if (UserGroupInformation.isSecurityEnabled()) {
      String userPrincipal = conf.get(DT_AUTH_PRINCIPAL);
      String userKeytab = conf.get(DT_AUTH_KEYTAB);
      authenticate(userPrincipal, userKeytab);
    }
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
      }
      catch (IOException ie) {
        LOG.error("Error login user with principal {}", principal, ie);
        throw ie;
      }
    }
  }

  public static long refreshTokens(long tokenLifeTime, String destinationDir, String destinationFile, final Configuration conf, String hdfsKeyTabFile, final Credentials credentials, final InetSocketAddress rmAddress, final boolean renewRMToken) throws IOException
  {
    long expiryTime = System.currentTimeMillis() + tokenLifeTime;
    //renew tokens
    final String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      throw new IOException(
        "Can't get Master Kerberos principal for the RM to use as renewer");
    }
    FileSystem fs = FileSystem.newInstance(conf);
    File keyTabFile;
    try {
      keyTabFile = FSUtil.copyToLocalFileSystem(fs, destinationDir, destinationFile, hdfsKeyTabFile, conf);
    }
    finally {
      fs.close();
    }
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(UserGroupInformation.getCurrentUser().getUserName(), keyTabFile.getAbsolutePath());
    try {
      ugi.doAs(new PrivilegedExceptionAction<Object>()
      {
        @Override
        public Object run() throws Exception
        {
          FileSystem fs1 = FileSystem.newInstance(conf);
          YarnClient yarnClient = null;
          if (renewRMToken) {
            yarnClient = YarnClient.createYarnClient();
            yarnClient.init(conf);
            yarnClient.start();
          }
          Credentials creds = new Credentials();
          try {
            fs1.addDelegationTokens(tokenRenewer, creds);
            if (renewRMToken) {
              new StramClientUtils.ClientRMHelper(yarnClient, conf).addRMDelegationToken(tokenRenewer, creds);
            }
          }
          finally {
            fs1.close();
            if (renewRMToken) {
              yarnClient.stop();
            }
          }
          credentials.addAll(creds);

          return null;
        }
      });
      UserGroupInformation.getCurrentUser().addCredentials(credentials);
    }
    catch (InterruptedException e) {
      LOG.error("Error while renewing tokens ", e);
      expiryTime = System.currentTimeMillis();
    }
    catch (IOException e) {
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
