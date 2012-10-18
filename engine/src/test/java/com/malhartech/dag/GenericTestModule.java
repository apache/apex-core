/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Module for constructing unit test DAG.
 * Test should reference the ports defined using the constants.
 */
@ModuleAnnotation(
    ports = {
        @PortAnnotation(name = GenericTestModule.INPUT1,  type = PortType.INPUT),
        @PortAnnotation(name = GenericTestModule.INPUT2,  type = PortType.INPUT),
        @PortAnnotation(name = GenericTestModule.OUTPUT1, type = PortType.OUTPUT)
    }
)
public class GenericTestModule extends Module {
  public static final String INPUT1 = "input1";
  public static final String INPUT2 = "input2";
  public static final String OUTPUT1 = "output1";

  private static final Logger LOG = LoggerFactory.getLogger(GenericTestModule.class);

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

  @Override
  public void process(Object o) {
    LOG.debug("Got some work: " + o);
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
