/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.security;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>StramUserLogin class.</p>
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 * @since 0.3.2
 */
public class StramUserLogin
{

  private static final Logger LOG = LoggerFactory.getLogger(StramUserLogin.class);
  private static final String STRAM_AUTH_PRINCIPAL = "stram.authentication.principal";
  private static final String STRAM_AUTH_KEYTAB = "stram.authentication.keytab";

  public static void attemptAuthentication(Configuration conf) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      String userPrincipal = conf.get(STRAM_AUTH_PRINCIPAL);
      String userKeytab = conf.get(STRAM_AUTH_KEYTAB);
      authenticate(userPrincipal, userKeytab);
    }
  }

  public static void authenticate(String principal, String keytab) throws IOException {
    if ((principal != null) && !principal.isEmpty()
            && (keytab != null) && !keytab.isEmpty()) {
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        LOG.info("Login user " + UserGroupInformation.getCurrentUser().getUserName());
      }
      catch (IOException ie) {
        LOG.error("Error login user with principal {}", principal, ie);
        throw ie;
      }
    }
  }
}
