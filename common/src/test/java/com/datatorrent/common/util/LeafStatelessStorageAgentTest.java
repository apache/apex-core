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
package com.datatorrent.common.util;

import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;

import com.datatorrent.api.annotation.Stateless;

public class LeafStatelessStorageAgentTest extends FSStorageAgentTest
{
  @Test
  public void testActivationCheckpointWrite() throws IOException
  {
    Map<Integer, String> activationData = Maps.newHashMap();
    activationData.put(1, "one");
    activationData.put(2, "two");

    // only saving initial state is allowed.
    LeafStatelessStorageAgent agent = new LeafStatelessStorageAgent(testMeta.applicationPath, null);
    agent.save(activationData, 1, Stateless.WINDOW_ID);
    FileContext fileContext = FileContext.getFileContext();
    Path ckPath = agent.getCheckpointPath(1, Stateless.WINDOW_ID);
    Assert.assertTrue("operator 1 window -1", fileContext.util().exists(ckPath));
    Assert.assertTrue("File contain some data", fileContext.getFileStatus(ckPath).getLen() > 0);

    // save at any other window, writes an empty file.
    Map<Integer, String> data = Maps.newHashMap();
    data.put(3, "three");
    data.put(4, "four");
    agent.save(data, 1, 1);
    ckPath = agent.getCheckpointPath(1, 1);
    Assert.assertTrue("operator 1 window 1", fileContext.util().exists(ckPath));
    Assert.assertEquals("file is empty", 0, fileContext.getFileStatus(ckPath).getLen());

    // load from any window returns the same state as activation window.
    Map<Integer, String> loaded = (Map<Integer, String>)agent.load(1, 1);
    Assert.assertEquals("data matches with activation checkpoint", activationData, loaded);

    // Test purge
    agent.delete(1, 1);
    ckPath = agent.getCheckpointPath(1, 1);
    Assert.assertFalse("operator 1 window 1 file does not exists", fileContext.util().exists(ckPath));
  }
}
