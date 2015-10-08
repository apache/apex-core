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
package com.datatorrent.stram.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.datatorrent.stram.util.ConfigUtils;


/**
 * Unit tests for StramClientUtils
 */
public class StramClientUtilsTest
{

  private String getHostString(String host) throws UnknownHostException
  {
    InetAddress address = InetAddress.getByName(host);
    if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
      return address.getCanonicalHostName();
    } else {
      return address.getHostName();
    }
  }

  @Test
  public void testRMWebAddress() throws UnknownHostException
  {
    Configuration conf = new Configuration(false);

    // basic test
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY, false);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "192.168.1.1:8032");
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "192.168.1.2:8032");
    Assert.assertEquals(getHostString("192.168.1.1") + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, null)));
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY, true);
    Assert.assertEquals(getHostString("192.168.1.2") + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, null)));
    
    // set localhost if host is unknown
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "someunknownhost.:8032");

    Assert.assertEquals(InetAddress.getLocalHost().getCanonicalHostName() + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, null)));

    // set localhost
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "127.0.0.1:8032");
    Assert.assertEquals(InetAddress.getLocalHost().getCanonicalHostName() + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, null)));

    // test when HA is enabled
    conf.getBoolean(ConfigUtils.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS + ".rm1", "192.168.1.1:8032");
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS + ".rm2", "192.168.1.2:8032");
    Assert.assertEquals(getHostString("192.168.1.1") + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, "rm1")));
    Assert.assertEquals(getHostString("192.168.1.2") + ":8032", StramClientUtils.getSocketConnectString(StramClientUtils.getRMWebAddress(conf, "rm2")));

  }

}
