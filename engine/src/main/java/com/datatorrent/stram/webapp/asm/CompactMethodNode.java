/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;

public class CompactMethodNode
{
  private MethodSignatureVisitor methodSignatureNode;
  
  private String name;

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public MethodSignatureVisitor getMethodSignatureNode()
  {
    return methodSignatureNode;
  }

  public void setMethodSignatureNode(MethodSignatureVisitor methodSignatureNode)
  {
    this.methodSignatureNode = methodSignatureNode;
  }
  

}
