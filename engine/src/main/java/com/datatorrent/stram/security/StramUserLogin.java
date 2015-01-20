/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.security;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.datatorrent.api.StreamingApplication;

/**
 * <p>StramUserLogin class.</p>
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 * @since 0.3.2
 */
public class StramUserLogin
{
  private static final Logger LOG = LoggerFactory.getLogger(StramUserLogin.class);
  private static final String DT_AUTH_PREFIX = StreamingApplication.DT_PREFIX + "authentication.";
  private static final String DT_AUTH_PRINCIPAL = DT_AUTH_PREFIX +  "principal";
  private static final String DT_AUTH_KEYTAB = DT_AUTH_PREFIX + "keytab";
  private static String principal;
  private static String keytab;

  public static void attemptAuthentication(Configuration conf) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      String userPrincipal = conf.get(DT_AUTH_PRINCIPAL);
      String userKeytab = conf.get(DT_AUTH_KEYTAB);
      authenticate(userPrincipal, userKeytab);
    }
  }

  public static void authenticate(String principal, String keytab) throws IOException {
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

  public static String getPrincipal()
  {
    return principal;
  }

  public static String getKeytab()
  {
    return keytab;
  }

}
