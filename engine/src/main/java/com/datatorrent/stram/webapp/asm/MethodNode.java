/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;

import org.objectweb.asm.signature.SignatureReader;


/**
 * A {@link org.objectweb.asm.tree.MethodNode} implementation to parse method signature as well
 *
 * @since 2.1
 */
public class MethodNode extends org.objectweb.asm.tree.MethodNode
{
  public GenericTypeSignatureVisitor typeVariableSignatureNode;
 
  public MethodSignatureVisitor signatureNode;

  public MethodNode()
  {
    super();
  }

  public MethodNode(int access, String name, String desc, String signature, String[] exceptions)
  {
    super(access, name, desc, signature, exceptions);
  }
  
  @Override
  public void visitEnd()
  {
    super.visitEnd();
    String methodString = signature != null ? signature : desc;
    // System.out.println(methodString);
    // System.out.println("RRRRRRRRRRRR" + methodString + "%%%%%" + name);
    SignatureReader reader = new SignatureReader(methodString);
    signatureNode = new MethodSignatureVisitor();
//    signatureNode.signature = methodString;
    signatureNode.typeV.addAll(typeVariableSignatureNode.typeV);
    reader.accept(signatureNode);
  }
  
  
  

}
