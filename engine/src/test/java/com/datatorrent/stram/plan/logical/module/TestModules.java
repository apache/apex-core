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

import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Module;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.stram.engine.GenericOperatorProperty;

public class TestModules
{

  public static class GenericModule implements Module
  {
    private static final Logger LOG = LoggerFactory.getLogger(TestModules.class);

    public volatile Object inport1Tuple = null;

    @OutputPortFieldAnnotation(optional = true)
    public final transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<>();

    @OutputPortFieldAnnotation(optional = true)
    public final transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<>();

    private String emitFormat;

    public boolean booleanProperty;

    private String myStringProperty;

    private transient GenericOperatorProperty genericOperatorProperty = new GenericOperatorProperty("test");

    public String getMyStringProperty()
    {
      return myStringProperty;
    }

    public void setMyStringProperty(String myStringProperty)
    {
      this.myStringProperty = myStringProperty;
    }

    public boolean isBooleanProperty()
    {
      return booleanProperty;
    }

    public void setBooleanProperty(boolean booleanProperty)
    {
      this.booleanProperty = booleanProperty;
    }

    public String propertySetterOnly;

    /**
     * setter w/o getter defined
     *
     * @param v
     */
    public void setStringPropertySetterOnly(String v)
    {
      this.propertySetterOnly = v;
    }

    public String getEmitFormat()
    {
      return emitFormat;
    }

    public void setEmitFormat(String emitFormat)
    {
      this.emitFormat = emitFormat;
    }

    public GenericOperatorProperty getGenericOperatorProperty()
    {
      return genericOperatorProperty;
    }

    public void setGenericOperatorProperty(GenericOperatorProperty genericOperatorProperty)
    {
      this.genericOperatorProperty = genericOperatorProperty;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.debug("populateDAG of module called");
    }
  }

  public static class ValidationTestModule implements Module
  {
    @NotNull
    @Pattern(regexp = ".*malhar.*", message = "Value has to contain 'malhar'!")
    private String stringField1;

    @Min(2)
    private int intField1;

    @AssertTrue(message = "stringField1 should end with intField1")
    private boolean isValidConfiguration()
    {
      return stringField1.endsWith(String.valueOf(intField1));
    }

    private String getterProperty2 = "";

    @NotNull
    public String getProperty2()
    {
      return getterProperty2;
    }

    public void setProperty2(String s)
    {
      // annotations need to be on the getter
      getterProperty2 = s;
    }

    private String[] stringArrayField;

    public String[] getStringArrayField()
    {
      return stringArrayField;
    }

    public void setStringArrayField(String[] stringArrayField)
    {
      this.stringArrayField = stringArrayField;
    }

    public class Nested
    {
      @NotNull
      private String property = "";

      public String getProperty()
      {
        return property;
      }

      public void setProperty(String property)
      {
        this.property = property;
      }

    }

    @Valid
    private final Nested nestedBean = new Nested();

    private String stringProperty2;

    public String getStringProperty2()
    {
      return stringProperty2;
    }

    public void setStringProperty2(String stringProperty2)
    {
      this.stringProperty2 = stringProperty2;
    }

    private Map<String, String> mapProperty = Maps.newHashMap();

    public Map<String, String> getMapProperty()
    {
      return mapProperty;
    }

    public void setMapProperty(Map<String, String> mapProperty)
    {
      this.mapProperty = mapProperty;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {

    }
  }

}
