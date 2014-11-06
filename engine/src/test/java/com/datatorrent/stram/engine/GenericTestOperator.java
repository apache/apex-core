/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Module for constructing unit test DAG.
 * Test should reference the ports defined using the constants.
 */
public class GenericTestOperator extends BaseOperator {

  private static final Logger LOG = LoggerFactory.getLogger(GenericTestOperator.class);

  public static final String IPORT1 = "inport1";
  public static final String IPORT2 = "inport2";
  public static final String OPORT1 = "outport1";
  public static final String OPORT2 = "outport2";

  public volatile Object inport1Tuple = null;

  @InputPortFieldAnnotation(optional=true)
  final public transient InputPort<Object> inport1 = new DefaultInputPort<Object>() {
    @Override
    final public void process(Object t) {
      inport1Tuple = t;
      processInternal(t);
    }
    @Override
    public String toString() {
      return GenericTestOperator.this.toString() + "." + IPORT1;
    }
  };

  @InputPortFieldAnnotation(optional=true)
  final public transient InputPort<Object> inport2 = new DefaultInputPort<Object>() {
    @Override
    final public void process(Object payload) {
      processInternal(payload);
    }
    @Override
    public String toString() {
      return GenericTestOperator.this.toString() + "." + IPORT2;
    }
  };

  @OutputPortFieldAnnotation(optional=true)
  final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>();

  @OutputPortFieldAnnotation(optional=true)
  final public transient DefaultOutputPort<Object> outport2 = new DefaultOutputPort<Object>();

  private String emitFormat;

  public boolean booleanProperty;

  private String myStringProperty;

  private transient GenericOperatorProperty genericOperatorProperty = new GenericOperatorProperty("test");

  public String getMyStringProperty() {
    return myStringProperty;
  }

  public void setMyStringProperty(String myStringProperty) {
    this.myStringProperty = myStringProperty;
  }

  public boolean isBooleanProperty() {
    return booleanProperty;
  }

  public void setBooleanProperty(boolean booleanProperty) {
    this.booleanProperty = booleanProperty;
  }

  public String propertySetterOnly;

  /**
   * setter w/o getter defined
   * @param v
   */
  public void setStringPropertySetterOnly(String v) {
    this.propertySetterOnly = v;
  }

  public String getEmitFormat() {
    return emitFormat;
  }

  public void setEmitFormat(String emitFormat) {
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

  private void processInternal(Object o) {
    LOG.debug("Got some work: " + o);
    if (emitFormat != null) {
      o = String.format(emitFormat, o);
    }
    if (outport1.isConnected()) {
      outport1.emit(o);
    }
  }

}
