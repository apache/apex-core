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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.stram.util.ConfigUtils;

/**
 * Borrowed from org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterIntializer
 * See https://issues.apache.org/jira/browse/YARN-1517
 *
 * @since 0.9.2
 */
public class StramWSFilterInitializer extends FilterInitializer
{
  private static final String FILTER_NAME = "AM_PROXY_FILTER";
  private static final String FILTER_CLASS = StramWSFilter.class.getCanonicalName();

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> params = new HashMap<String, String>();
    String proxy = WebAppUtils.getProxyHostAndPort(conf);
    String[] parts = proxy.split(":");
    params.put(StramWSFilter.PROXY_HOST, parts[0]);
    params.put(StramWSFilter.PROXY_URI_BASE,
            ConfigUtils.getSchemePrefix(new YarnConfiguration()) + proxy +
                    System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV));
    container.addFilter(FILTER_NAME, FILTER_CLASS, params);
  }
}
