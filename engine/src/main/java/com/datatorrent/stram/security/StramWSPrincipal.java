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
package com.datatorrent.stram.security;

import java.security.Principal;

/**
 * Based on org.apache.hadoop.yarn.server.webproxy.amfilter.AmIPPrincipal
 * See https://issues.apache.org/jira/browse/YARN-1516
 *
 * @since 0.9.2
 */
public class StramWSPrincipal implements Principal
{
  private final String name;

  public StramWSPrincipal(String name)
  {
    this.name = name;
  }

  @Override
  public String getName()
  {
    return name;
  }
}
