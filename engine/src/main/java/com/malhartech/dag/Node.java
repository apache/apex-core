/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.malhartech.dag;

/**
 *
 * @author chetan
 */
public interface Node extends DAGPart<NodeConfiguration, NodeContext> {
    public void setup(NodeConfiguration config);
    public void beginWindow(NodeContext context);
    public void endWindow(NodeContext context);
    public void process(NodeContext context,  StreamContext streamContext, Object payload);
    public void teardown();
}
