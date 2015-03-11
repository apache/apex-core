package com.datatorrent.stram.webapp.asm;


import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.ClassNode;

public class ClassNodeType extends ClassNode
{
  
  @SuppressWarnings("unchecked")
  @Override
  public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
  {
    MethodNode mn = new MethodNode(access, name, desc, signature, exceptions);
    methods.add(mn);
    return mn;
  }
  
  
  @Override
  public void visitEnd()
  {
    super.visitEnd();
    
  }
  

}
