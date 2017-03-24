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

package com.example.mydtapp;

import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

/**
 * This is a test module which can be added to DAG . It will add one dummy operator as well.
 */
public class TestModule implements Module
{
  private String testInput;

  public final transient ProxyOutputPort<String> outputs = new ProxyOutputPort<String>();

  public final transient ProxyInputPort<String> inputs = new ProxyInputPort<String>();

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    if (testInput == null) {
      throw new NullPointerException();
    }

    TestModuleOperator testModuleOperator = dag.addOperator("testModuleOperator", TestModuleOperator.class);
    testModuleOperator.setTestInput(this.getTestInput());
  }

  public String getTestInput()
  {
    return testInput;
  }

  public void setTestInput(String testInput)
  {
    this.testInput = testInput;
  }
}
