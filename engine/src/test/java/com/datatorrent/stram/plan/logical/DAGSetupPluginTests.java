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
package com.datatorrent.stram.plan.logical;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.engine.util.StreamingAppFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;

public class DAGSetupPluginTests
{

  public static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      TestGeneratorInputOperator inputOperator = dag.addOperator("inputOperator", new TestGeneratorInputOperator());
      GenericTestOperator operator1 = dag.addOperator("operator1", new GenericTestOperator());
      GenericTestOperator operator2 = dag.addOperator("operator2", new GenericTestOperator());
      GenericTestOperator operator3 = dag.addOperator("operator3", new GenericTestOperator());
      GenericTestOperator operator4 = dag.addOperator("operator4", new GenericTestOperator());

      dag.addStream("n1n2", operator1.outport1, operator2.inport1);
      dag.addStream("inputStream", inputOperator.outport, operator1.inport1, operator3.inport1, operator4.inport1);
    }
  }

  private Configuration getConfiguration()
  {
    Configuration conf = new Configuration();
    conf.set(DAGSetupPluginManager.DAGSETUP_PLUGINS_CONF_KEY, "com.datatorrent.stram.plan.logical.PropertyInjectorVisitor");
    conf.set("propertyVisitor.Path","/visitortests.properties");
    return conf;
  }

  @Test
  public void testJavaApplication()
  {
    Configuration conf = getConfiguration();
    StreamingAppFactory factory  = new StreamingAppFactory(Application.class.getName(), Application.class)
    {
      @Override
      public LogicalPlan createApp(LogicalPlanConfiguration planConfig)
      {
        Class<? extends StreamingApplication> c = StramUtils.classForName(Application.class.getName(), StreamingApplication.class);
        StreamingApplication app = StramUtils.newInstance(c);
        return super.createApp(app, planConfig);
      }
    };
    LogicalPlan dag = factory.createApp(new LogicalPlanConfiguration(conf));
    validateProperties(dag);
  }

  @Test
  public void testPropertyFileApp() throws IOException
  {
    File tempFile = File.createTempFile("testTopology", "properties");
    org.apache.commons.io.IOUtils.copy(getClass().getResourceAsStream("/testTopology.properties"), new FileOutputStream(tempFile));
    StramAppLauncher.PropertyFileAppFactory factory = new StramAppLauncher.PropertyFileAppFactory(tempFile);
    Configuration conf = getConfiguration();
    LogicalPlan dag = factory.createApp(new LogicalPlanConfiguration(conf));
    validateProperties(dag);
    tempFile.delete();
  }

  @Test
  public void testJsonFileApp() throws IOException
  {
    File tempFile = File.createTempFile("testTopology", "json");
    org.apache.commons.io.IOUtils.copy(getClass().getResourceAsStream("/testTopology.json"), new FileOutputStream(tempFile));
    StramAppLauncher.JsonFileAppFactory factory = new StramAppLauncher.JsonFileAppFactory(tempFile);
    Configuration conf = getConfiguration();
    LogicalPlan dag = factory.createApp(new LogicalPlanConfiguration(conf));
    validateProperties(dag);
    tempFile.delete();
  }

  protected void validateProperties(LogicalPlan dag)
  {
    String[] operators = new String[]{"operator1", "operator2", "operator3", "operator4"};
    for (String name : operators) {
      GenericTestOperator op = (GenericTestOperator)dag.getOperatorMeta(name).getOperator();
      Assert.assertEquals("property set on operator ", op.getMyStringProperty(), "mynewstringvalue");
    }
  }
}
