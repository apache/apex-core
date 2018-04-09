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
package org.apache.apex.engine.security;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Maps;

/**
 *
 *
 * @since 3.7.0
 */
public class ACLManager
{

  private static final Logger logger = LoggerFactory.getLogger(ACLManager.class);

  public static void setupUserACLs(ContainerLaunchContext launchContext, String userName, Configuration conf) throws IOException
  {
    logger.debug("Setup login acls {}", userName);
    if (areACLsRequired(conf)) {
      logger.debug("Configuring ACLs for {}", userName);
      Map<ApplicationAccessType, String> acls = Maps.newHashMap();
      acls.put(ApplicationAccessType.VIEW_APP, userName);
      acls.put(ApplicationAccessType.MODIFY_APP, userName);
      launchContext.setApplicationACLs(acls);
    }
  }

  public static boolean areACLsRequired(Configuration conf)
  {
    logger.debug("Check ACLs required");
    if (conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE, YarnConfiguration.DEFAULT_YARN_ACL_ENABLE)) {
      logger.debug("Admin ACL {}", conf.get(YarnConfiguration.YARN_ADMIN_ACL));
      if (!YarnConfiguration.DEFAULT_YARN_ADMIN_ACL.equals(conf.get(YarnConfiguration.YARN_ADMIN_ACL))) {
        logger.debug("Non default admin ACL");
        return true;
      }
    }
    return false;
  }

}
