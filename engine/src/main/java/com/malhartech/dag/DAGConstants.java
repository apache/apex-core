/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

/**
 *
 */
public interface DAGConstants {

  /**
   * Internal use only, set by application launcher.
   * The name under which the application master expects its configuration.
   */
  public static final String SER_FILE_NAME = "stram-conf.ser";

  /**
   * Name under which the application will be shown in the resource manager.
   * If not set, the default is the configuration Java class or property file name.
   */
  public static final String STRAM_APPNAME = "stram.appName";
  /**
   * Comma separated list of jar file dependencies to be deployed with the application.
   * The launcher will combine the list with built-in dependencies and those specified
   * via {@link com.malhartech.annotation.ShipContainingJars} into the final set of resources
   * that are made available through the distributed file system to application master
   * and child containers.
   */
  public static final String STRAM_LIBJARS = "stram.libjars";

  /**
   * The maximum number or containers (excluding the application master) that the application is allowed to request.
   * If the DAG plan requires less containers, remaining count won't be allocated from the resource manager.
   * Example: DAG with several operators and all inline streams would require one container,
   * only one container will be requested from the resource manager.
   */
  public static final String STRAM_MAX_CONTAINERS = "stram.maxContainers";

  /**
   * Dump extra debug information in launcher, master and containers.
   */
  public static final String STRAM_DEBUG = "stram.debug";


  /**
   * Launch mode for the application.
   * Used in the client to set configuration depending on how the DAG is executed.
   */
  public static final String STRAM_LAUNCH_MODE = "stram.launchmode";


  /**
   * The amount of memory to be requested for streaming containers. Not used in local mode.
   */
  public static final String STRAM_CONTAINER_MEMORY_MB = "stram.containerMemoryMB";
  public static final String STRAM_CONTAINER_JVM_OPTS = "stram.containerJvmOpts";

  /**
   * The amount of memory to be requested for the application master. Not used in local mode.
   */
  public static final String STRAM_MASTER_MEMORY_MB = "stram.masterMemoryMB";

  public static final String STRAM_WINDOW_SIZE_MILLIS = "stram.windowSizeMillis";
  public static final String STRAM_CHECKPOINT_DIR = "stram.checkpointDir";
  public static final String STRAM_CHECKPOINT_INTERVAL_MILLIS = "stram.checkpointIntervalMillis";

}
