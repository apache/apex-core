/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.webapp.asm;

import java.util.LinkedList;
import java.util.List;

import org.apache.xbean.asm5.signature.SignatureVisitor;

/**
 * Follow the visiting path of ASM
 * to decompose method signature to data structure
 *
 * ClassSignature = ( visitFormalTypeParameter visitClassBound? visitInterfaceBound* )* ( visitSuperClass
 * visitInterface* )
 * MethodSignature = ( visitFormalTypeParameter visitClassBound? visitInterfaceBound* )* ( visitParameterType*
 * visitReturnType visitExceptionType* )
 * TypeSignature = visitBaseType | visitTypeVariable | visitArrayType | ( visitClassType visitTypeArgument* (
 * visitInnerClassType visitTypeArgument* )* visitEnd ) )
 *
 * @since 2.1
 */
public class ClassSignatureVisitor extends BaseSignatureVisitor
{

  public enum END
  {
    CLASSNAME, SUPERCLASS, INTERFACE
  }

  private Type superClass;

  private List<Type> interfaces;

  private END end = END.CLASSNAME;

  @Override
  public SignatureVisitor visitExceptionType()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SignatureVisitor visitParameterType()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SignatureVisitor visitReturnType()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SignatureVisitor visitSuperclass()
  {
    visitingStack.clear();
    end = END.SUPERCLASS;
    return this;
  }

  @Override
  public void visitClassType(String classType)
  {
    super.visitClassType(classType);
  }

  @Override
  public SignatureVisitor visitInterface()
  {
    // could be superclass before this
    if (!visitingStack.isEmpty() && end == END.SUPERCLASS) {
      superClass = visitingStack.pop();
    }
    // could be another interface before this
    if (interfaces == null) {
      interfaces = new LinkedList<>();
    }
    if (end == END.INTERFACE) {
      interfaces.add(0, visitingStack.pop());
    }
    end = END.INTERFACE;
    return this;
  }

  public List<Type> getInterfaces()
  {
    if (interfaces == null) {
      interfaces = new LinkedList<>();
    }
    if (interfaces != null && end == END.INTERFACE && !visitingStack.isEmpty()) {
      interfaces.add(0, visitingStack.pop());
    }
    return interfaces;
  }

  public Type getSuperClass()
  {

    if (superClass == null && end == END.SUPERCLASS) {
      superClass = visitingStack.pop();
    }
    return superClass;
  }

}
