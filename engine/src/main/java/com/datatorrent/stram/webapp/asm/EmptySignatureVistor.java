/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;

import org.objectweb.asm.signature.SignatureVisitor;

/**
 * An empty {@link SignatureVisitor} implementation which stop the parent visitor from parsing the whole signature
 * It is important because we only need to partially parse the signature
 * @since 2.1
 */
public class EmptySignatureVistor implements SignatureVisitor
{

  @Override
  public SignatureVisitor visitArrayType()
  {
    return this;
  }

  @Override
  public void visitBaseType(char arg0)
  {

  }

  @Override
  public SignatureVisitor visitClassBound()
  {
    return this;
  }

  @Override
  public void visitClassType(String arg0)
  {

  }

  @Override
  public void visitEnd()
  {

  }

  @Override
  public SignatureVisitor visitExceptionType()
  {
    return this;
  }

  @Override
  public void visitFormalTypeParameter(String arg0)
  {

  }

  @Override
  public void visitInnerClassType(String arg0)
  {

  }

  @Override
  public SignatureVisitor visitInterface()
  {
    return this;
  }

  @Override
  public SignatureVisitor visitInterfaceBound()
  {
    return this;
  }

  @Override
  public SignatureVisitor visitParameterType()
  {
    return this;
  }

  @Override
  public SignatureVisitor visitReturnType()
  {
    return this;
  }

  @Override
  public SignatureVisitor visitSuperclass()
  {
    return this;
  }

  @Override
  public void visitTypeArgument()
  {

  }

  @Override
  public SignatureVisitor visitTypeArgument(char arg0)
  {
    return this;
  }

  @Override
  public void visitTypeVariable(String arg0)
  {

  }

}
