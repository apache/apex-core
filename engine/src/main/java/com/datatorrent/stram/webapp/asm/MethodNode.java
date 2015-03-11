package com.datatorrent.stram.webapp.asm;

import org.objectweb.asm.signature.SignatureReader;


public class MethodNode extends org.objectweb.asm.tree.MethodNode
{
 
  public MethodSignatureVisitor signatureNode;

  public MethodNode()
  {
    super();
    // TODO Auto-generated constructor stub
  }

  public MethodNode(int access, String name, String desc, String signature, String[] exceptions)
  {
    super(access, name, desc, signature, exceptions);
    String methodString = signature!=null ? signature : desc;
//    System.out.println(methodString);
//    System.out.println("RRRRRRRRRRRR" + methodString + "%%%%%" + name);
    SignatureReader reader = new SignatureReader(methodString);
    signatureNode = new MethodSignatureVisitor();
    reader.accept(signatureNode);
  }
  
  
  

}
