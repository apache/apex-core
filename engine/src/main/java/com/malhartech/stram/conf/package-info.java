/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 * 
 */

/**
 * 
 * <b>com.malhartech.stram.conf</b> package deals with parsing the dag specification and construct dag structure.<p>
 * <br>
 * The same code is used both client side and in stram. This ensures that the client side compilation is exactly same as in stram. Any design checks, rules enforcement etc.
 * would thus be uniforming enforced on hadoop client side as well as in stram<br>
 * The com.malhartech.stram.webapp package consists of<br>
 * <b>TBD: Lots of classes to be added here, but waiting for correct naming convention</b>
 * <b>{@link com.malhartech.stram.conf.ShipContainingJars}</b>: Annotation to indicate Stram a jar file dependency that needs to be deployed to cluster<br>
 * <b>{@link com.malhartech.stram.conf.TopologyBuilder}</b>: Builder for the DAG logical representation of nodes and streams<br>
 * <br>
 * <br>
 * 
 */
package com.malhartech.stram.conf;


