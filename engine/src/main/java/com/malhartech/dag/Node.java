/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

/**
 *
 * @author chetan
 */
public interface Node {
    public void setup();
    public void beginWindow(long window);
    public void endWidndow(long window);
    public void process(Tuple t);
    public void teardown();
}
