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
package org.apache.apex.engine.security;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

import com.datatorrent.api.Context;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.util.FSUtil;

public class TokenRenewer
{

  // The constant is not available hence defining here. If in future it is available this can be removed
  private static final Text HDFS_TOKEN_KIND = new Text("HDFS_DELEGATION_TOKEN");

  private static final Logger logger = LoggerFactory.getLogger(TokenRenewer.class);

  boolean renewRMToken;
  Configuration conf;
  String destinationFile;

  long tokenLifeTime;
  long tokenRenewalInterval;
  String principal;
  String hdfsKeyTabFile;
  InetSocketAddress rmAddress;

  long expiryTime;
  long renewTime;
  Credentials credentials;

  public TokenRenewer(Context context, boolean renewRMToken, Configuration conf, String destinationFile) throws IOException
  {
    this.renewRMToken = renewRMToken;
    this.destinationFile = destinationFile;
    this.conf = conf;

    if (renewRMToken) {
      tokenLifeTime = (long)(context.getValue(LogicalPlan.TOKEN_REFRESH_ANTICIPATORY_FACTOR) * Math.min(context.getValue(LogicalPlan.HDFS_TOKEN_LIFE_TIME), context.getValue(LogicalPlan.RM_TOKEN_LIFE_TIME)));
      tokenRenewalInterval = (long)(context.getValue(LogicalPlan.TOKEN_REFRESH_ANTICIPATORY_FACTOR) * Math.min(context.getValue(LogicalPlan.HDFS_TOKEN_RENEWAL_INTERVAL), context.getValue(LogicalPlan.RM_TOKEN_RENEWAL_INTERVAL)));
      rmAddress = conf.getSocketAddr(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
    } else {
      tokenLifeTime = (long)(context.getValue(LogicalPlan.TOKEN_REFRESH_ANTICIPATORY_FACTOR) * context.getValue(LogicalPlan.HDFS_TOKEN_LIFE_TIME));
      tokenRenewalInterval = (long)(context.getValue(LogicalPlan.TOKEN_REFRESH_ANTICIPATORY_FACTOR) * context.getValue(LogicalPlan.HDFS_TOKEN_RENEWAL_INTERVAL));
    }

    principal = context.getValue(LogicalPlan.PRINCIPAL);
    hdfsKeyTabFile = context.getValue(LogicalPlan.KEY_TAB_FILE);

    expiryTime = System.currentTimeMillis() + tokenLifeTime;
    renewTime = expiryTime;

    logger.debug("token life time {} renewal interval {}", tokenLifeTime, tokenRenewalInterval);
    logger.debug("Token expiry time {} renew time {}", expiryTime, renewTime);

    credentials = UserGroupInformation.getCurrentUser().getCredentials();
    // Check credentials are proper at RM
    if (renewRMToken) {
      renewTokens(false, true);
    }
  }

  public void checkAndRenew() throws IOException
  {
    boolean renew = false;
    boolean refresh = false;
    long currentTimeMillis = System.currentTimeMillis();
    if (currentTimeMillis >= expiryTime && hdfsKeyTabFile != null) {
      refresh = true;
    } else if (currentTimeMillis >= renewTime) {
      renew = true;
    }
    if (refresh || renew) {
      long updateTime = renewTokens(refresh, false);
      if (refresh) {
        expiryTime = updateTime;
        renewTime = currentTimeMillis + tokenRenewalInterval;
        logger.debug("Token expiry time {} renew time {}", expiryTime, renewTime);
      } else {
        renewTime = updateTime;
        logger.debug("Token renew time {}", renewTime);
      }
    }
  }

  private long renewTokens(final boolean refresh, boolean checkOnly) throws IOException
  {
    logger.info("{}", checkOnly ? "Checking renewal" : (refresh ? "Refreshing tokens" : "Renewing tokens"));
    long expiryTime = System.currentTimeMillis() + (refresh ? tokenLifeTime : tokenRenewalInterval);

    final String tokenRenewer = UserGroupInformation.getCurrentUser().getUserName();
    logger.debug("Token renewer {}", tokenRenewer);

    File keyTabFile = null;
    try (FileSystem fs = FileSystem.newInstance(conf)) {
      String destinationDir = FileUtils.getTempDirectoryPath();
      keyTabFile = FSUtil.copyToLocalFileSystem(fs, destinationDir, destinationFile, hdfsKeyTabFile, conf);

      if (principal == null) {
        //principal = UserGroupInformation.getCurrentUser().getUserName();
        principal = UserGroupInformation.getLoginUser().getUserName();
      }
      logger.debug("Principal {}", principal);
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTabFile.getAbsolutePath());
      if (!checkOnly) {
        try {
          UserGroupInformation currUGI = UserGroupInformation.createProxyUser(tokenRenewer, ugi);
          currUGI.doAs(new PrivilegedExceptionAction<Object>()
          {
            @Override
            public Object run() throws Exception
            {

              if (refresh) {
                Credentials creds = new Credentials();
                try (FileSystem fs1 = FileSystem.newInstance(conf)) {
                  logger.info("Refreshing fs tokens");
                  fs1.addDelegationTokens(tokenRenewer, creds);
                  logger.info("Refreshed tokens");
                }
                if (renewRMToken) {
                  try (YarnClient yarnClient = StramClientUtils.createYarnClient(conf)) {
                    logger.info("Refreshing rm tokens");
                    new StramClientUtils.ClientRMHelper(yarnClient, conf).addRMDelegationToken(tokenRenewer, creds);
                    logger.info("Refreshed tokens");
                  }
                }
                credentials.addAll(creds);
              } else {
                Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
                for (Token<? extends TokenIdentifier> token : tokens) {
                  logger.debug("Token {}", token);
                  if (token.getKind().equals(HDFS_TOKEN_KIND) || (renewRMToken && token.getKind().equals(RMDelegationTokenIdentifier.KIND_NAME))) {
                    logger.info("Renewing token {}", token.getKind());
                    token.renew(conf);
                    logger.info("Renewed token");
                  }
                }
              }

              return null;
            }
          });
          UserGroupInformation.getCurrentUser().addCredentials(credentials);
        } catch (InterruptedException e) {
          logger.error("Error while renewing tokens ", e);
          expiryTime = System.currentTimeMillis();
        } catch (IOException e) {
          logger.error("Error while renewing tokens ", e);
          expiryTime = System.currentTimeMillis();
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("number of tokens: {}", credentials.getAllTokens().size());
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while (iter.hasNext()) {
          Token<?> token = iter.next();
          logger.debug("updated token: {}", token);
        }
      }
    } finally {
      if (keyTabFile != null) {
        keyTabFile.delete();
      }
    }
    return expiryTime;
  }

}
