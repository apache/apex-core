/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.conf;

/**
 *
 */
public interface TopologyConstants {

  /**
   * Comma separated list of jar files that will be made available to stram app master and child containers
   */
  public static final String STRAM_LIBJARS = "stram.libjars";
  public static final String STRAM_MAX_CONTAINERS = "stram.numContainers";
  public static final String STRAM_DEBUG = "stram.debug";
  public static final String STRAM_CONTAINER_MEMORY_MB = "stram.containerMemoryMB";
  public static final String STRAM_MASTER_MEMORY_MB = "stram.masterMemoryMB";
  public static final String STRAM_WINDOW_SIZE_MILLIS = "stram.windowSizeMillis";
  public static final String STRAM_CHECKPOINT_DIR = "stram.checkpointDir";
  public static final String STRAM_CHECKPOINT_INTERVAL_MILLIS = "stram.checkpointIntervalMillis"; 
  
  
}
