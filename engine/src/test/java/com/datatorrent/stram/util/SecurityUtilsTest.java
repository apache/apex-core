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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.stram.security.AuthScheme;

/**
 *
 */
public class SecurityUtilsTest
{
  @Test
  public void testStramWebSecurity()
  {
    Configuration conf = setupConfiguration(null);
    checkSecurityConfiguration(conf, new boolean[][]{{false, false}, {false, true}, {false, false}, {false, false}, {false, false}});
    conf = setupConfiguration(AuthScheme.SPNEGO);
    checkSecurityConfiguration(conf, new boolean[][]{{true, false}, {true, true}, {true, false}, {true, false}, {true, true}});
  }

  @Test
  public void testBasicAuth() throws NoSuchFieldException, IllegalAccessException
  {
    testAuthScheme(AuthScheme.BASIC);
  }

  @Test
  public void testDigestAuth() throws NoSuchFieldException, IllegalAccessException
  {
    testAuthScheme(AuthScheme.DIGEST);
  }

  private void testAuthScheme(AuthScheme authScheme) throws NoSuchFieldException, IllegalAccessException
  {
    Configuration conf = setupConfiguration(authScheme);
    SecurityUtils.init(conf);
    WebServicesClientTest.checkUserCredentials("testuser", "testpass", authScheme);
  }

  private Configuration setupConfiguration(AuthScheme authScheme)
  {
    Configuration conf = new Configuration();
    if (authScheme != null) {
      conf.set(SecurityUtils.HADOOP_HTTP_AUTH_PROP, authScheme.getName());
      conf.addResource("security/dt-site-" + authScheme.getName() + ".xml");
    }
    return conf;
  }

  private void checkSecurityConfiguration(Configuration conf, boolean[][] securityConf)
  {
    Assert.assertEquals("Number variations", 5, securityConf.length);
    SecurityUtils.init(conf);
    checkWebSecurity(securityConf[0][0], securityConf[0][1]);
    SecurityUtils.init(conf, Context.StramHTTPAuthentication.ENABLE);
    checkWebSecurity(securityConf[1][0], securityConf[1][1]);
    SecurityUtils.init(conf, Context.StramHTTPAuthentication.DISABLE);
    checkWebSecurity(securityConf[2][0], securityConf[2][1]);
    SecurityUtils.init(conf, Context.StramHTTPAuthentication.FOLLOW_HADOOP_AUTH);
    checkWebSecurity(securityConf[3][0], securityConf[3][1]);
    SecurityUtils.init(conf, Context.StramHTTPAuthentication.FOLLOW_HADOOP_HTTP_AUTH);
    checkWebSecurity(securityConf[4][0], securityConf[4][1]);
  }

  private void checkWebSecurity(boolean hadoopWebSecurity, boolean stramWebSecurity)
  {
    Assert.assertEquals("Hadoop web security", hadoopWebSecurity, SecurityUtils.isHadoopWebSecurityEnabled());
    Assert.assertEquals("Stram web security", stramWebSecurity, SecurityUtils.isStramWebSecurityEnabled());
  }
}
