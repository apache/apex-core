/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  public ClassSignatureVisitor typeVariableSignatureNode;
 
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
