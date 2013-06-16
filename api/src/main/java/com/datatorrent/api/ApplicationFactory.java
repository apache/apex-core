/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.api;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface to be implemented for Java based streaming application declaration.
 * <p>
 * An application is the top level DAG with external configuration for
 * application master / engine settings, application specific properties or
 * overrides for individual operators in the DAG. <br>
 * Application launchers (CLI) use the interface to identify application DAGs
 * within jar files and supply the configuration upon instantiation.
 * <p>
 * Operator properties in the DAG can be configured externally. When an
 * application is launched from the CLI, any settings in stram-site.xml would
 * override property values in the DAG. It is therefore possible to have
 * defaults in the DAG code and supply environment/launch context specific
 * settings through the configuration.
 */
public interface ApplicationFactory {
    public static final String LAUNCHMODE_YARN = "yarn";
    public static final String LAUNCHMODE_LOCAL = "local";

    void populateDAG(DAG dag, Configuration conf);
}
