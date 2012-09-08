/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;

/**
 * Node for topology testing.
 * Test should reference the ports defined using the constants.
 */
@NodeAnnotation(
    ports = {
        @PortAnnotation(name = GenericTestNode.INPUT1,  type = PortType.INPUT),
        @PortAnnotation(name = GenericTestNode.INPUT2,  type = PortType.INPUT),
        @PortAnnotation(name = GenericTestNode.OUTPUT1, type = PortType.OUTPUT)
    }
)
public class GenericTestNode extends AbstractNode {
  public static final String INPUT1 = "input1";
  public static final String INPUT2 = "input2";
  public static final String OUTPUT1 = "output1";

  private static final Logger LOG = LoggerFactory.getLogger(GenericTestNode.class);

  private String emitFormat;

  boolean booleanProperty;

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

  @Override
  public void process(Object o) {
    LOG.info("Got some work: " + o);
    if (emitFormat != null) {
      o = String.format(emitFormat, o);
    }
    emit(o);
  }

  @Override
  public void handleIdleTimeout()
  {
//    deactivate();
  }
}
