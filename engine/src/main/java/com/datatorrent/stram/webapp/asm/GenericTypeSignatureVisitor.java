/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;

import org.objectweb.asm.signature.SignatureVisitor;

/**
 * Follow the visiting path of ASM
 * to visit getter and setter method signature
 * 
 * ClassSignature = ( visitFormalTypeParameter visitClassBound? visitInterfaceBound* )* ( visitSuperClass visitInterface* )
 * MethodSignature = ( visitFormalTypeParameter visitClassBound? visitInterfaceBound* )* ( visitParameterType* visitReturnType visitExceptionType* )
 * TypeSignature = visitBaseType | visitTypeVariable | visitArrayType | ( visitClassType visitTypeArgument* ( visitInnerClassType visitTypeArgument* )* visitEnd ) )
 */
public class GenericTypeSignatureVisitor extends BaseSignatureVisitor
{

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
    return new EmptySignatureVistor();
  }

  
  @Override
  public SignatureVisitor visitInterface()
  {
    return new EmptySignatureVistor();
  }

}
