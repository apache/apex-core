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

import com.datatorrent.api.Context.StramHTTPAuthentication;
import com.datatorrent.stram.security.AuthScheme;
import com.datatorrent.stram.security.StramUserLogin;

/**
 *
 *
 * @since 3.4.0
 */
public class SecurityUtils
{

  public static final String HADOOP_HTTP_AUTH_PROP = "hadoop.http.authentication.type";
  private static final String HADOOP_HTTP_AUTH_VALUE_SIMPLE = "simple";

  // If not initialized explicitly using init call, default to Hadoop auth for backwards compatibility
  private static boolean stramWebSecurityEnabled = UserGroupInformation.isSecurityEnabled();
  private static boolean hadoopWebSecurityEnabled = stramWebSecurityEnabled;

  public static void init(Configuration configuration)
  {
    init(configuration, null);
  }

  public static void init(Configuration configuration, StramHTTPAuthentication stramHTTPAuth)
  {
    hadoopWebSecurityEnabled = false;
    String authValue = configuration.get(HADOOP_HTTP_AUTH_PROP);
    if ((authValue != null) && !authValue.equals(HADOOP_HTTP_AUTH_VALUE_SIMPLE)) {
      hadoopWebSecurityEnabled = true;
      initAuth(configuration);
    }
    // Stram http auth may not be specified and is null but still set a default
    if (stramHTTPAuth == StramHTTPAuthentication.FOLLOW_HADOOP_HTTP_AUTH) {
      stramWebSecurityEnabled = hadoopWebSecurityEnabled;
    } else if (stramHTTPAuth == StramHTTPAuthentication.ENABLE) {
      stramWebSecurityEnabled = true;
    } else if (stramHTTPAuth == StramHTTPAuthentication.DISABLE) {
      stramWebSecurityEnabled = false;
    } else {
      // Default to StramHTTPAuthentication.FOLLOW_HADOOP_AUTH behavior
      stramWebSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    }
  }

  private static void initAuth(final Configuration configuration)
  {
    // Authentication scheme is not unambiguously known because in Hadoop authentication, authentication type can be
    // specified as an implementation class, furthermore authentication types like SASL wrap other mechanisms like BASIC
    // or SPNEGO underneath and the wrapped scheme is not known till the authentication negotiation process
    WebServicesClient.initAuth(new WebServicesClient.ConfigProvider()
    {
      @Override
      public String getProperty(AuthScheme scheme, String name)
      {
        StringBuilder propNamesb = new StringBuilder(StramUserLogin.DT_AUTH_PREFIX).append(scheme.getName()).append(".").append(name);
        return configuration.get(propNamesb.toString());
      }
    });
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
