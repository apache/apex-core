/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.util;

import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class ConfigUtils
{

  public static InetSocketAddress getRMAddress(YarnConfiguration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                                              YarnConfiguration.DEFAULT_RM_ADDRESS,
                                              YarnConfiguration.DEFAULT_RM_PORT);
  }

  public static String getRMUsername(Configuration conf) {
    String principal = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      principal = conf.get(YarnConfiguration.RM_PRINCIPAL);
      int sindex = -1;
      if ((principal != null) && ((sindex =principal.indexOf('/')) != -1)) {
        principal = principal.substring(0,sindex);
      }
    }
    return principal;
  }
}
