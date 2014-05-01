package com.datatorrent.stram.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.stram.util.ConfigUtils;

/**
 * Created by pramod on 12/17/13.
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
            ConfigUtils.getSchemePrefix(new Configuration()) + proxy +
                    System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV));
    container.addFilter(FILTER_NAME, FILTER_CLASS, params);
  }
}
