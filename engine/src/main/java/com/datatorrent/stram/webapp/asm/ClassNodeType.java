package com.datatorrent.stram.webapp.asm;


import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.ClassNode;

public class ClassNodeType extends ClassNode
{
  
  
  
  @Override
  public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
  {
    // TODO Auto-generated method stub
    return new MethodNode(access, name, desc, signature, exceptions);
  }
  

}
