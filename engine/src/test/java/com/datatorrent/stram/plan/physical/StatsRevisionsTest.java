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
package com.datatorrent.stram.plan.physical;

import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.stram.plan.physical.StatsRevisions.VersionedLong;

/**
 *
 */
public class StatsRevisionsTest
{
  @Test
  public void test()
  {
    StatsRevisions revs = new StatsRevisions();
    VersionedLong vl = revs.newVersionedLong();

    long v = vl.get();
    Assert.assertTrue("initial value", v == 0);

    revs.checkout();
    vl.set(5);

    v = vl.get();
    Assert.assertTrue("new value after set", v == 5);

    revs.commit();

    v = vl.get();
    Assert.assertTrue("new value after commit", v == 5);

    try {
      vl.set(5);
      Assert.fail("modify readonly revision");
    } catch (AssertionError ve) {
      // expected
    }

  }

}
