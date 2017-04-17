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
package org.apache.apex.engine.util;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.common.util.FSStorageAgent;

public class CascadeStorageAgentTest
{

  static class TestMeta extends TestWatcher
  {
    String applicationPath;

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
  public void testSingleIndirection() throws IOException
  {
    String oldAppPath = testMeta.applicationPath;
    FSStorageAgent storageAgent = new FSStorageAgent(oldAppPath, null);
    storageAgent.save("1", 1, 1);
    storageAgent.save("2", 1, 2);
    storageAgent.save("3", 2, 1);

    String newAppPath = oldAppPath + ".new";
    CascadeStorageAgent cascade = new CascadeStorageAgent(storageAgent, new FSStorageAgent(newAppPath, null));
    long[] operatorIds = cascade.getWindowIds(1);
    Assert.assertArrayEquals("Returned window ids ", operatorIds, new long[]{1L, 2L});

    operatorIds = cascade.getWindowIds(2);
    Assert.assertArrayEquals("Returned window ids ", operatorIds, new long[]{1L});

    /* save should happen to new location */
    cascade.save("4", 1, 4);
    FileContext fileContext = FileContext.getFileContext();
    Assert.assertFalse("operator 1 window 4 file does not exists in old directory", fileContext.util().exists(new Path(oldAppPath + "/" + 1 + "/" + 4)));
    Assert.assertTrue("operator 1 window 4 file exists in new directory", fileContext.util().exists(new Path(newAppPath + "/" + 1 + "/" + 4)));

    // check for delete,
    // delete for old checkpoint should be ignored
    cascade.save("5", 1, 5);
    cascade.delete(1, 2L);
    Assert.assertTrue("operator 1 window 2 file exists in old directory", fileContext.util().exists(new Path(oldAppPath + "/" + 1 + "/" + 2)));
    cascade.delete(1, 4L);
    Assert.assertFalse("operator 1 window 4 file does not exists in old directory", fileContext.util().exists(new Path(newAppPath + "/" + 1 + "/" + 4)));

    /* chaining of storage agent */
    String latestAppPath = oldAppPath + ".latest";
    cascade = new CascadeStorageAgent(storageAgent, new FSStorageAgent(newAppPath, null));
    CascadeStorageAgent latest = new CascadeStorageAgent(cascade, new FSStorageAgent(latestAppPath, null));
    operatorIds = latest.getWindowIds(1);
    Assert.assertArrayEquals("Window ids ", operatorIds, new long[] {1,2,5});

    latest.save("6", 1, 6);
    Assert.assertFalse("operator 1 window 6 file does not exists in old directory", fileContext.util().exists(new Path(oldAppPath + "/" + 1 + "/" + 6)));
    Assert.assertFalse("operator 1 window 6 file does not exists in old directory", fileContext.util().exists(new Path(newAppPath + "/" + 1 + "/" + 6)));
    Assert.assertTrue("operator 1 window 6 file exists in new directory", fileContext.util().exists(new Path(latestAppPath + "/" + 1 + "/" + 6)));
  }
}
