/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;

/**
 * Store method information only needed by app builder
 * @since 2.1
 */
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
