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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.StramHTTPAuthentication;

/**
 *
 *
 * @since 3.4.0
 */
public class SecurityUtils
{

  public static final String HADOOP_HTTP_AUTH_PROP = "hadoop.http.authentication.type";
  private static final String HADOOP_HTTP_AUTH_VALUE_SIMPLE = "simple";

  private static boolean stramWebSecurityEnabled;
  private static boolean hadoopWebSecurityEnabled;

  // If not initialized explicitly default to Hadoop auth
  static {
    hadoopWebSecurityEnabled = stramWebSecurityEnabled = UserGroupInformation.isSecurityEnabled();
  }

  public static void init(Configuration configuration, StramHTTPAuthentication stramHTTPAuth)
  {
    hadoopWebSecurityEnabled = false;
    String authValue = configuration.get(HADOOP_HTTP_AUTH_PROP);
    if ((authValue != null) && !authValue.equals(HADOOP_HTTP_AUTH_VALUE_SIMPLE)) {
      hadoopWebSecurityEnabled = true;
    }
    // Stram http auth may not be specified and is null but still set a default
    boolean authDefault = false;
    if (stramHTTPAuth != null) {
      if (stramHTTPAuth == Context.StramHTTPAuthentication.FOLLOW_HADOOP_HTTP_AUTH) {
        stramWebSecurityEnabled = hadoopWebSecurityEnabled;
      } else if (stramHTTPAuth == StramHTTPAuthentication.FOLLOW_HADOOP_AUTH) {
        stramWebSecurityEnabled = UserGroupInformation.isSecurityEnabled();
      } else if (stramHTTPAuth == StramHTTPAuthentication.ENABLE) {
        stramWebSecurityEnabled = true;
      } else if (stramHTTPAuth == StramHTTPAuthentication.DISABLE) {
        stramWebSecurityEnabled = false;
      }
    }
  }

  public static boolean isHadoopWebSecurityEnabled()
  {
    return hadoopWebSecurityEnabled;
  }

  public static boolean isStramWebSecurityEnabled()
  {
    return stramWebSecurityEnabled;
  }

}
