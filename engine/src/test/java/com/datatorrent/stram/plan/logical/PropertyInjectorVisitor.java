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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.validation.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.plugin.DAGSetupEvent;
import org.apache.apex.api.plugin.DAGSetupPlugin;
import org.apache.apex.api.plugin.Plugin.EventHandler;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

import static org.apache.apex.api.plugin.DAGSetupEvent.Type.PRE_VALIDATE_DAG;

public class PropertyInjectorVisitor implements DAGSetupPlugin<DAGSetupPlugin.Context>, EventHandler<DAGSetupEvent>
{
  private static final Logger LOG = LoggerFactory.getLogger(PropertyInjectorVisitor.class);

  private String path;
  private Map<String, String> propertyMap = new HashMap<>();
  private DAG dag;

  @Override
  public void setup(DAGSetupPlugin.Context context)
  {
    this.dag = context.getDAG();
    try {
      this.path = context.getConfiguration().get("propertyVisitor.Path");
      Properties properties = new Properties();
      properties.load(this.getClass().getResourceAsStream(path));
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        propertyMap.put(entry.getKey().toString(), entry.getValue().toString());
      }
    } catch (IOException ex) {
      throw new ValidationException("Not able to load input file " + path);
    }
    context.register(PRE_VALIDATE_DAG, this);
  }

  @Override
  public void handle(DAGSetupEvent event)
  {
    for (DAG.OperatorMeta ometa : dag.getAllOperatorsMeta()) {
      Operator o = ometa.getOperator();
      LogicalPlanConfiguration.setOperatorProperties(o, propertyMap);
    }
  }

  public PropertyInjectorVisitor()
  {
  }

  public String getPath()
  {
    return path;
  }

  public void setPath(String path)
  {
    this.path = path;
  }

  @Override
  public void teardown()
  {

  }
}
