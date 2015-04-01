/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;


import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.tree.ClassNode;

/**
 * A special org.objectweb.asm.tree.ClassNode implementation which parse the type signature as well 
 * @see ClassNode
 * @since 2.1
 */
public class ClassNodeType extends ClassNode
{
  
  GenericTypeSignatureVisitor gtsv = new GenericTypeSignatureVisitor();
  
  @SuppressWarnings("unchecked")
  @Override
  public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
  {
    MethodNode mn = new MethodNode(access, name, desc, signature, exceptions);
    mn.typeVariableSignatureNode = gtsv;
    methods.add(mn);
    return mn;
  }
  
  
  @Override
  public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
  {
    // parse the signature first so Type variable can be captured from the signature
    if(signature!=null){
      SignatureReader sr = new SignatureReader(signature);
//      gtsv.signature = signature;
      sr.accept(gtsv);
    }
    super.visit(version, access, name, signature, superName, interfaces);
  }
  

}
