/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.DAG;
import org.apache.hadoop.conf.Configuration;

/**
 * Interface to be implemented by custom classes for Java based application declaration.
 * The interface is used by the CLI to identify Java DAG configuration classes within jar files.
 */
public interface ApplicationFactory {
    public static final String LAUNCHMODE_YARN = "yarn";
    public static final String LAUNCHMODE_LOCAL = "local";

    DAG getApplication(Configuration conf);
}
