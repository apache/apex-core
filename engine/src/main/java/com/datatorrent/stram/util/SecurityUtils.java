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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.SSLConfig;
import com.datatorrent.api.Context.StramHTTPAuthentication;
import com.datatorrent.stram.security.AuthScheme;
import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.security.StramWSFilterInitializer;

/**
 *
 *
 * @since 3.4.0
 */
public class SecurityUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);
  @VisibleForTesting
  protected static final String HADOOP_HTTP_AUTH_PROP = "hadoop.http.authentication.type";
  private static final String HADOOP_HTTP_AUTH_VALUE_SIMPLE = "simple";
  private static final String HADOOP_HTTP_AUTH_SIMPLE_ANONYMOUS_ALLOWED_PROP = "hadoop.http.authentication.simple.anonymous.allowed";
  /**
   * Config property name for the SSL keystore location used by Hadoop webapps.
   * This should be replaced when a constant is defined there
   */
  private static final String SSL_SERVER_KEYSTORE_LOCATION = "ssl.server.keystore.location";

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

  @VisibleForTesting
  protected static boolean isStramWebSecurityEnabled()
  {
    return stramWebSecurityEnabled;
  }

  /**
   * Setup security related configuration for {@link org.apache.hadoop.yarn.webapp.WebApp}.
   * @param config
   * @param sslConfig
   * @return
   */
  public static Configuration configureWebAppSecurity(Configuration config, SSLConfig sslConfig)
  {
    if (isStramWebSecurityEnabled()) {
      config = new Configuration(config);
      config.set("hadoop.http.filter.initializers", StramWSFilterInitializer.class.getCanonicalName());
    } else {
      String authType = config.get(HADOOP_HTTP_AUTH_PROP);
      if (!HADOOP_HTTP_AUTH_VALUE_SIMPLE.equals(authType)) {
        // turn off authentication for Apex as specified by user
        LOG.warn("Found {} {} but authentication was disabled in Apex.", HADOOP_HTTP_AUTH_PROP, authType);
        config = new Configuration(config);
        config.set(HADOOP_HTTP_AUTH_PROP, HADOOP_HTTP_AUTH_VALUE_SIMPLE);
        config.setBoolean(HADOOP_HTTP_AUTH_SIMPLE_ANONYMOUS_ALLOWED_PROP, true);
      }
    }
    if (sslConfig != null) {
      addSSLConfigResource(config, sslConfig);
    }
    return config;
  }

  /**
   * Modify config object by adding SSL related parameters into a resource for WebApp's use
   *
   * @param config  Configuration to be modified
   * @param sslConfig
   */
  private static void addSSLConfigResource(Configuration config, SSLConfig sslConfig)
  {
    String nodeLocalConfig = sslConfig.getConfigPath();
    if (StringUtils.isNotEmpty(nodeLocalConfig)) {
      config.addResource(new Path(nodeLocalConfig));
    } else {
      // create a configuration object and add it as a resource
      Configuration sslConfigResource = new Configuration(false);
      final String SSL_CONFIG_LONG_NAME = Context.DAGContext.SSL_CONFIG.getLongName();
      sslConfigResource.set(SSL_SERVER_KEYSTORE_LOCATION, new Path(sslConfig.getKeyStorePath()).getName(), SSL_CONFIG_LONG_NAME);
      sslConfigResource.set(WebAppUtils.WEB_APP_KEYSTORE_PASSWORD_KEY, sslConfig.getKeyStorePassword(), SSL_CONFIG_LONG_NAME);
      sslConfigResource.set(WebAppUtils.WEB_APP_KEY_PASSWORD_KEY, sslConfig.getKeyStoreKeyPassword(), SSL_CONFIG_LONG_NAME);
      config.addResource(sslConfigResource);
    }
  }

}
