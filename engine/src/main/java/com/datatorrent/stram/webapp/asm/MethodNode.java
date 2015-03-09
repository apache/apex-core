package com.datatorrent.stram.webapp.asm;

import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;


public class MethodNode extends org.objectweb.asm.tree.MethodNode
{
 
  public SignatureVisitor signatureNode;

  public MethodNode()
  {
    super();
    // TODO Auto-generated constructor stub
  }

  public MethodNode(int access, String name, String desc, String signature, String[] exceptions)
  {
    super(access, name, desc, signature, exceptions);
    String methodString = signature!=null ? signature : desc;
//    System.out.println("RRRRRRRRRRRR" + methodString + "%%%%%" + name);
    SignatureReader reader = new SignatureReader(methodString);
    signatureNode = new MethodSignatureVisitor();
    reader.accept(signatureNode);
  }
  
  
  

}
