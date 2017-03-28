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
package com.datatorrent.stram.plan.logical.module;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class TestModuleProperties
{
  @Test
  public void testModuleProperties()
  {
    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o1.prop.myStringProperty", "myStringPropertyValue");
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.prop.stringArrayField", "a,b,c");
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.prop.mapProperty.key1", "key1Val");
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.prop.mapProperty(key1.dot)", "key1dotVal");
    conf.set(StreamingApplication.APEX_PREFIX + "operator.o2.prop.mapProperty(key2.dot)", "key2dotVal");

    LogicalPlan dag = new LogicalPlan();
    TestModules.GenericModule o1 = dag.addModule("o1", new TestModules.GenericModule());
    TestModules.ValidationTestModule o2 = dag.addModule("o2", new TestModules.ValidationTestModule());

    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(conf);

    pb.setModuleProperties(dag, "testSetOperatorProperties");
    Assert.assertEquals("o1.myStringProperty", "myStringPropertyValue", o1.getMyStringProperty());
    Assert.assertArrayEquals("o2.stringArrayField", new String[]{"a", "b", "c"}, o2.getStringArrayField());

    Assert.assertEquals("o2.mapProperty.key1", "key1Val", o2.getMapProperty().get("key1"));
    Assert.assertEquals("o2.mapProperty(key1.dot)", "key1dotVal", o2.getMapProperty().get("key1.dot"));
    Assert.assertEquals("o2.mapProperty(key2.dot)", "key2dotVal", o2.getMapProperty().get("key2.dot"));

  }

}
