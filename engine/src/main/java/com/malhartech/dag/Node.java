/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.malhartech.stram.StreamingNodeContext;

/**
 *
 * @author chetan
 */
public interface Node {
    public void setup(StreamingNodeContext ctx);
    public void beginWindow(long window);
    public void endWidndow(long window);
    public void process(Tuple t);
    public void teardown(StreamingNodeContext ctx);
}
