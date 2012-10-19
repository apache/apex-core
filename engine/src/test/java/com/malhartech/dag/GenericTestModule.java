/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 * Module for constructing unit test DAG.
 * Test should reference the ports defined using the constants.
 */
public class GenericTestModule extends BaseOperator {
  public static final String IPORT1 = "input1";
  public static final String IPORT2 = "input2";
  public static final String OPORT1 = "output1";

  private static final Logger LOG = LoggerFactory.getLogger(GenericTestModule.class);

  @InputPortFieldAnnotation(name=IPORT1)
  final public transient InputPort<Object> inport1 = new DefaultInputPort<Object>(this) {
    @Override
    final public void process(Object payload) {
      processInternal(payload);
    }
  };

  @InputPortFieldAnnotation(name=IPORT2)
  final public transient InputPort<Object> inport2 = new DefaultInputPort<Object>(this) {
    @Override
    final public void process(Object payload) {
      processInternal(payload);
    }
  };

  @OutputPortFieldAnnotation(name=OPORT1)
  final public transient DefaultOutputPort<Object> outport1 = new DefaultOutputPort<Object>(this);

  private String emitFormat;

  public boolean booleanProperty;

  private String myStringProperty;

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

  public String getEmitFormat() {
    return emitFormat;
  }

  public void setEmitFormat(String emitFormat) {
    this.emitFormat = emitFormat;
  }

  private void processInternal(Object o) {
    LOG.debug("Got some work: " + o);
    if (emitFormat != null) {
      o = String.format(emitFormat, o);
    }
    outport1.emit(o);
  }

}
