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
package com.datatorrent.stram.engine;

import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

/**
 * Tests for {@link OperatorContext}
 */
public class OperatorContextTest
{
  private static CountDownLatch latch = new CountDownLatch(1);
  private static volatile String operatorName;

  private static class MockInputOperator extends BaseOperator implements InputOperator
  {
    @Override
    public void setup(Context.OperatorContext context)
    {
      operatorName = Preconditions.checkNotNull(context.getName(), "operator name");
      latch.countDown();
    }

    @Override
    public void emitTuples()
    {
    }
  }

  @Test
  public void testInjectionOfOperatorName() throws Exception
  {
    StreamingApplication application = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("input", new MockInputOperator());
      }
    };
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(application, new Configuration());
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    latch.await();
    Assert.assertEquals("operator name", "input", operatorName);
    lc.shutdown();
  }
}
