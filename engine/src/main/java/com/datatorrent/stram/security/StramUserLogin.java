/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
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
  private static final String STRAM_USER_PRINCIPAL = "stram.user.principal";
  private static final String STRAM_USER_KEYTAB = "stram.user.keytab";

  public static void attemptAuthentication(Configuration conf) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      String userPrincipal = conf.get(STRAM_USER_PRINCIPAL);
      String userKeytab = conf.get(STRAM_USER_KEYTAB);
      if ((userPrincipal != null) && !userPrincipal.isEmpty()
              && (userKeytab != null) && !userKeytab.isEmpty()) {
        try {
          UserGroupInformation.loginUserFromKeytab(userPrincipal, userKeytab);
          LOG.info("Login user " + UserGroupInformation.getCurrentUser().getUserName());
        }
        catch (IOException ie) {
          LOG.error("Error login user with principal {}", userPrincipal, ie);
          throw ie;
        }
      }
    }
  }
}
