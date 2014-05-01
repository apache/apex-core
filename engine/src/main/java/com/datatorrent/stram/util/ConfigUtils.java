/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * <p>ConfigUtils class.</p>
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 * @since 0.3.2
 */
public class ConfigUtils
{

  public static InetSocketAddress getRMAddress(YarnConfiguration conf)
  {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_PORT);
  }

  public static String getRMUsername(Configuration conf)
  {
    String principal = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      principal = conf.get(YarnConfiguration.RM_PRINCIPAL);
      int sindex = -1;
      if ((principal != null) && ((sindex = principal.indexOf('/')) != -1)) {
        principal = principal.substring(0, sindex);
      }
    }
    return principal;
  }

  public static String getSchemePrefix(YarnConfiguration conf)
  {
    if( HttpConfig.Policy.HTTPS_ONLY == HttpConfig.Policy.fromString(conf
      .get(YarnConfiguration.YARN_HTTP_POLICY_KEY, YarnConfiguration.YARN_HTTP_POLICY_DEFAULT))){
      return "https://";
    }else{
      return "http://";
    }
  }
}
