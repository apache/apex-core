/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.security;

import java.io.IOException;
import java.util.HashMap;
import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class KerberosAuth
{

  public static Subject loginUser(String principal, char[] password) throws LoginException, IOException {
    Subject subject = new Subject();
    LoginContext lc = new LoginContext(com.datatorrent.stram.security.KerberosAuth.class.getName(), subject, new AuthenticationHandler(principal, password),
                                                                            new KerberosConfiguration(principal));
    lc.login();
    return subject;
    //return UserGroupInformation.getUGIFromTicketCache(ticketCache, principal);
  }

  private static class AuthenticationHandler implements CallbackHandler {

    private String principal;
    private char[] password;

    AuthenticationHandler(String principal, char[] password) {
      this.principal = principal;
      this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
    {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nameCallback = (NameCallback)callback;
          nameCallback.setName(principal);
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback passwordCallback = (PasswordCallback)callback;
          passwordCallback.setPassword(password);
        }
      }
    }

  }

  private static class KerberosConfiguration extends Configuration {

    private String principal;

    KerberosConfiguration(String principal) {
      this.principal = principal;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      if (name.equals(com.datatorrent.stram.security.KerberosAuth.class.getName())) {
        AppConfigurationEntry[] configEntries = new AppConfigurationEntry[1];
        HashMap<String,String> params = new HashMap<String, String>();
        params.put("useTicketCache", "true");
        params.put("principal", principal);
        configEntries[0] = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", LoginModuleControlFlag.REQUIRED, params );
        return configEntries;
      } else {
        return null;
      }
    }
  }

  /*
  public static void main(String[] args) {
    try {
      Subject subject = KerberosAuth.loginUser("pramod/admin@MALHAR.COM", "password".toCharArray());
      System.out.println("Credentials " + subject.getPrincipals());
      System.out.println("Tokens " + subject.getPrivateCredentials());
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
  */
}
