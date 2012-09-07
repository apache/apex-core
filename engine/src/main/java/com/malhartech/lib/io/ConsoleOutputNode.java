/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;

/**
 *
 * Writes tuples to stdout of the container<p>
 * <br>
 * Mainly to be used for debugging. Users should be careful to not have this node listen to a high throughput stream<br>
 * <br>
 *
 */
@NodeAnnotation(
    ports = {
        @PortAnnotation(name = "input", type = PortType.INPUT)
    }
)
public class ConsoleOutputNode extends AbstractNode
{
  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t)
  {
    System.out.println(t);
  }

}
