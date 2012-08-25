/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.annotation;

import com.malhartech.annotation.PortAnnotation.PortType;


@NodeAnnotation(
    ports = {
        @PortAnnotation(name = "input1",  type = PortType.INPUT),
        @PortAnnotation(name = "input2",  type = PortType.INPUT),
        @PortAnnotation(name = "output1", type = PortType.OUTPUT)
    }
)
public class AnnotatedNode {

  void printPorts() {
    
      Class<?> clazz = this.getClass();
      NodeAnnotation na = clazz.getAnnotation(NodeAnnotation.class);
      if (na != null) {
        PortAnnotation[] ports = na.ports();
        for (PortAnnotation pa : ports) {
          System.out.println("name=" + pa.name() + ", type=" + pa.type());
        }
      }
  }
  
  public static void main(String[] args) {

    new AnnotatedNode().printPorts();
    
  }
  
  
}