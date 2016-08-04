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

import javax.validation.constraints.NotNull;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.api.OperatorDeployInfo;

public class OperatorContextTest
{
  private static String operatorName;

  @Test
  public void testInjectionOfOperatorName() throws Exception
  {
    final LocalMode lma = LocalMode.newInstance();
    StreamingApplication testApp = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.addOperator("input", new MockInputOperator());
      }
    };

    lma.prepareDAG(testApp, new Configuration());
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    long startTime = System.currentTimeMillis();
    while (operatorName == null && System.currentTimeMillis() - startTime < 2000) {
      Thread.sleep(100);
    }
    Assert.assertEquals("operator name", "input", operatorName);
    lc.shutdown();
  }

  private static class MockInputOperator extends BaseOperator implements InputOperator
  {

    @Override
    public void setup(Context.OperatorContext context)
    {
      operatorName = Preconditions.checkNotNull(context.getName(), "operator name");
    }

    @Override
    public void emitTuples()
    {
    }
  }

  /**
   * Returns an operator context instance that can be used by tests
   *
   * @param id            operator id
   * @param name          operator name
   * @param attributeMap  operator attributes
   * @param parentContext parent context
   * @return operator context
   */
  public static OperatorContext getTestOperatorContext(int id, @NotNull String name,
      @NotNull Attribute.AttributeMap attributeMap, Context parentContext)
  {
    OperatorDeployInfo odi = new OperatorDeployInfo();
    odi.id = id;
    odi.name = Preconditions.checkNotNull(name, "name");
    odi.contextAttributes = Preconditions.checkNotNull(attributeMap, "attributes");

    return new OperatorContext(odi, parentContext);
  }
}
