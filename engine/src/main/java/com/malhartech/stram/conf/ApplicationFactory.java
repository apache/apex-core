/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.conf;

/**
 * Interface to be implemented by custom classes for Java based application declaration.
 */
public interface ApplicationFactory {
    DAG getApplication();
}
