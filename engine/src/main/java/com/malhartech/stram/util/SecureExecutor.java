/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.util;

import java.io.IOException;
import java.security.PrivilegedAction;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Need to tie this in with some type of annotation for processing a secure workload
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class SecureExecutor
{
  public static <T> T execute(final SecureExecutor.WorkLoad<T> workLoad) throws IOException {
     if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      return loginUser.doAs(new PrivilegedAction<T>() {
        @Override
        public T run() {
          return workLoad.run();
        }
      });
     } else{
       return workLoad.run();
     }
  }

  public static interface WorkLoad<T> {
    public T run();
  }

}
