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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.util.ConfigUtils;

/**
 * Based on org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterIntializer
 * See https://issues.apache.org/jira/browse/YARN-1517
 *
 * @since 0.9.2
 */
public class StramWSFilterInitializer extends FilterInitializer
{
  private static final Logger logger = LoggerFactory.getLogger(StramWSFilterInitializer.class);

  private static final String FILTER_NAME = "AM_PROXY_FILTER";
  private static final String FILTER_CLASS = StramWSFilter.class.getCanonicalName();

  @Override
  public void initFilter(FilterContainer container, Configuration conf)
  {
    logger.debug("Conf {}", conf);
    Map<String, String> params = new HashMap<>();
    Collection<String> proxies = new ArrayList<>();
    if (ConfigUtils.isRMHAEnabled(conf)) {
      // HA is enabled get all
      for (String rmId : ConfigUtils.getRMHAIds(conf)) {
        proxies.add(getResolvedRMWebAppURLWithoutScheme(conf, rmId));
      }
      logger.info("HA proxy addresses {}", proxies);
    }
    if (proxies.isEmpty()) {
      proxies.add(getProxyHostAndPort(conf));
      logger.info("Proxy addresses {}", proxies);
    }
    StringBuilder proxyBr = new StringBuilder();
    for (String proxy : proxies) {
      if (proxyBr.length() != 0) {
        proxyBr.append(StramWSFilter.PROXY_DELIMITER);
      }
      String[] parts = proxy.split(":");
      proxyBr.append(parts[0]);
    }
    params.put(StramWSFilter.PROXY_HOST, proxyBr.toString());
    container.addFilter(FILTER_NAME, FILTER_CLASS, params);
  }

  /*
    From org.apache.hadoop.yarn.webapp.util.WebAppUtils
    Reimplementing it as audience for the WebAppUtils is private
    Using HA enabled methods below
  */
  public String getProxyHostAndPort(Configuration conf)
  {
    String addr = conf.get(YarnConfiguration.PROXY_ADDRESS);
    logger.info("proxy address setting {}", addr);
    logger.debug("proxy setting sources {}", conf.getPropertySources(YarnConfiguration.PROXY_ADDRESS));
    if (addr == null || addr.isEmpty()) {
      addr = getResolvedRMWebAppURLWithoutScheme(conf, null);
    }
    return addr;
  }

  /*
    From org.apache.hadoop.yarn.webapp.util.WebAppUtils
    Modified for HA support
    Replace with methods from Hadoop when HA support is available
    HttpConfig is not used as it's audience is private as well and it's interface has changed from Hadoop 2.2 to 2.6
  */
  public String getResolvedRMWebAppURLWithoutScheme(Configuration conf, String rmId)
  {
    InetSocketAddress socketAddress = StramClientUtils.getRMWebAddress(conf, rmId);
    return StramClientUtils.getSocketConnectString(socketAddress);
  }

  /*
    From org.apache.hadoop.yarn.webapp.util.WebAppUtils
    Modified for HA support
  */
  public String getResolvedRMWebAppURLWithoutScheme(Configuration conf, boolean sslEnabled, String rmId)
  {
    InetSocketAddress socketAddress = StramClientUtils.getRMWebAddress(conf, sslEnabled, rmId);
    return StramClientUtils.getSocketConnectString(socketAddress);
  }

}
