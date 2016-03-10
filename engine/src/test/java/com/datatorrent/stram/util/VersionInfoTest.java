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

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Operator;

/**
 *
 */
public class VersionInfoTest
{

  @Test
  public void testCompareVersion()
  {
    int c = VersionInfo.compare("1.0", "1.1");
    Assert.assertTrue(c < 0);
    c = VersionInfo.compare("1.10", "1.2");
    Assert.assertTrue(c > 0);
    c = VersionInfo.compare("1.0", "1.0");
    Assert.assertTrue(c == 0);
    c = VersionInfo.compare("1.0-SNAPSHOT", "1.0");
    Assert.assertTrue(c == 0);
    c = VersionInfo.compare("asdb", "1.0");
    Assert.assertTrue(c < 0);
  }

  @Test
  public void testCompatibleVersion()
  {
    Assert.assertFalse(VersionInfo.isCompatible("1.0", "1.1"));
    Assert.assertTrue(VersionInfo.isCompatible("1.10", "1.2"));
    Assert.assertTrue(VersionInfo.isCompatible("1.10.0", "1.10.34"));
    Assert.assertTrue(VersionInfo.isCompatible("1.10.55", "1.10.3"));
    Assert.assertTrue(VersionInfo.isCompatible("1.10.55", "1.10.55"));
    Assert.assertFalse(VersionInfo.isCompatible("1.10.55", "2.10.55"));
    Assert.assertFalse(VersionInfo.isCompatible("2.10.55", "1.10.55"));
  }

  @Test
  public void testMavenProperties()
  {
    VersionInfo v = new VersionInfo(Operator.class, "org.apache.apex", "apex-api", "git-properties-unavailable");
    if (!v.getVersion().matches("[0-9]+.[0-9]+.[0-9]+.*")) {
      Assert.fail("Version number pattern does not match: " + v.getVersion());
    }
  }

}
