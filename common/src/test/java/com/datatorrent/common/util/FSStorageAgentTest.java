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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;

public class FSStorageAgentTest
{
  private static class TestMeta extends TestWatcher
  {
    String applicationPath;
    FSStorageAgent storageAgent;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      try {
        FileUtils.forceMkdir(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      storageAgent = new FSStorageAgent(applicationPath, null);

      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_PATH, applicationPath);
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testSave() throws IOException
  {
    Map<Integer, String> data = Maps.newHashMap();
    data.put(1, "one");
    data.put(2, "two");
    data.put(3, "three");
    testMeta.storageAgent.save(data, 1, 1);
    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded = (Map<Integer, String>)testMeta.storageAgent.load(1, 1);
    Assert.assertEquals("dataOf1", data, decoded);
  }

  @Test
  public void testLoad() throws IOException
  {
    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    Map<Integer, String> dataOf2 = Maps.newHashMap();
    dataOf2.put(4, "four");
    dataOf2.put(5, "five");
    dataOf2.put(6, "six");

    testMeta.storageAgent.save(dataOf1, 1, 1);
    testMeta.storageAgent.save(dataOf2, 2, 1);
    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded1 = (Map<Integer, String>)testMeta.storageAgent.load(1, 1);

    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded2 = (Map<Integer, String>)testMeta.storageAgent.load(2, 1);
    Assert.assertEquals("data of 1", dataOf1, decoded1);
    Assert.assertEquals("data of 2", dataOf2, decoded2);
  }

  @Test
  public void testRecovery() throws IOException
  {
    testSave();
    testMeta.storageAgent = new FSStorageAgent(testMeta.applicationPath, null);
    testSave();
  }

  @Test
  public void testDelete() throws IOException
  {
    testLoad();

    testMeta.storageAgent.delete(1, 1);
    Path appPath = new Path(testMeta.applicationPath);
    FileContext fileContext = FileContext.getFileContext();
    Assert.assertTrue("operator 2 window 1", fileContext.util().exists(new Path(appPath + "/" + 2 + "/" + 1)));
    Assert.assertFalse("operator 1 window 1", fileContext.util().exists(new Path(appPath + "/" + 1 + "/" + 1)));
  }

}
