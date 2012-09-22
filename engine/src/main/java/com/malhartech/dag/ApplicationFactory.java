/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

/**
 * Interface to be implemented by custom classes for Java based application declaration.
 * The interface is used by the CLI to identify Java DAG configuration classes within jar files.
 */
public interface ApplicationFactory {
    DAG getApplication();
}
