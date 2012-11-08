/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface to be implemented for Java based streaming application declaration. <br>
 * An application is the top level DAG with external configuration for
 * application master / engine settings, application specific properties or
 * overrides for individual operators in the DAG. <br>
 * Application launchers (CLI) use the interface to identify application DAGs
 * within jar files and supply the configuration upon instantiation.
 */
public interface ApplicationFactory {
    public static final String LAUNCHMODE_YARN = "yarn";
    public static final String LAUNCHMODE_LOCAL = "local";

    DAG getApplication(Configuration conf);
}
