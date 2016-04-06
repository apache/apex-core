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
package com.datatorrent.stram.util;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.security.UserGroupInformation;

/**
 * Need to tie this in with some type of annotation for processing a secure workload
 *
 * @since 0.3.2
 */
public class SecureExecutor
{
  public static <T> T execute(final SecureExecutor.WorkLoad<T> workLoad) throws IOException
  {
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      return loginUser.doAs(new PrivilegedAction<T>()
      {
        @Override
        public T run()
        {
          return workLoad.run();
        }
      });
    } else {
      return workLoad.run();
    }
  }

  public interface WorkLoad<T>
  {
    T run();
  }

}
