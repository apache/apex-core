/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.stram.conf.StreamingApplicationFactory;
import com.malhartech.stram.conf.Topology;

/**
 * Test class for java application configuration
 */
public class TestAppFactory implements StreamingApplicationFactory {

  @Override
  public Topology getStreamingApplication() {
    throw new UnsupportedOperationException();
  }

}
