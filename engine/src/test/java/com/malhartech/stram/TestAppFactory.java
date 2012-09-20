/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.stram.conf.ApplicationFactory;
import com.malhartech.stram.conf.DAG;

/**
 * Test class for java application configuration
 */
public class TestAppFactory implements ApplicationFactory {

  @Override
  public DAG getApplication() {
    throw new UnsupportedOperationException();
  }

}
