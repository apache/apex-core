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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * Module for constructing unit test DAG.
 * Test should reference the ports defined using the constants.
 */
public class GenericTestOperator extends BaseOperator
{

  private static final Logger LOG = LoggerFactory.getLogger(GenericTestOperator.class);

  public static final String IPORT1 = "inport1";
  public static final String IPORT2 = "inport2";
  public static final String OPORT1 = "outport1";
  public static final String OPORT2 = "outport2";

  public volatile Object inport1Tuple = null;

  @InputPortFieldAnnotation(optional = true)
  public final transient InputPort<Object> inport1 = new DefaultInputPort<Object>()
  {
    @Override
    public final void process(Object t)
    {
      inport1Tuple = t;
      processInternal(t);
    }

    @Override
    public String toString()
    {
      return GenericTestOperator.this.toString() + "." + IPORT1;
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public final transient InputPort<Object> inport2 = new DefaultInputPort<Object>()
  {
    @Override
    public final void process(Object payload)
    {
      processInternal(payload);
    }

    @Override
    public String toString()
    {
      return GenericTestOperator.this.toString() + "." + IPORT2;
    }
  };

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

  private void processInternal(Object o)
  {
    LOG.debug("Got some work: " + o);
    if (emitFormat != null) {
      o = String.format(emitFormat, o);
    }
    if (outport1.isConnected()) {
      outport1.emit(o);
    }
    if (outport2.isConnected()) {
      outport2.emit(o);
    }
  }

}
