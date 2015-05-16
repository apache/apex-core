/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;

import java.util.LinkedList;
import java.util.List;

import org.objectweb.asm.signature.SignatureVisitor;

/**
 * A method signature parser
 *
 * @since 2.1.0
 */
public class MethodSignatureVisitor extends BaseSignatureVisitor
{
  
  // There is at most 1 parameter for setter and getter method
  private List<Type> parameters = new LinkedList<Type>();
  
  private Type returnType;
  
  private List<Type> exceptionType = new LinkedList<Type>();
  
  public static final int VISIT_PARAM = 1;
  
  public static final int VISIT_RETURN = 2;
  
  public static final int VISIT_EXCEPTION = 3;
  
  
  @Override
  public SignatureVisitor visitExceptionType()
  {
    //System.out.print("visitExceptionType　");
    if(stage == VISIT_RETURN && !visitingStack.isEmpty()){
      returnType = visitingStack.pop();
    }
    if(stage == VISIT_EXCEPTION && !visitingStack.isEmpty()){
      exceptionType.add(visitingStack.pop());
    }
    stage = VISIT_EXCEPTION;
    
    return this;
  }
  
  @Override
  public SignatureVisitor visitParameterType()
  {
    if(stage == VISIT_FORMAL_TYPE){
      stage = VISIT_PARAM;
      if(!visitingStack.isEmpty()){
        visitingStack.pop();
      };
      return this;
    } 
    stage = VISIT_PARAM;
    if(!visitingStack.isEmpty()){
      parameters.add(visitingStack.pop());
    };
    return this;
  }
  
  
  @Override
  public SignatureVisitor visitReturnType()
  {

    while(!visitingStack.isEmpty()){
      parameters.add(visitingStack.pop());
    }
    stage = VISIT_RETURN;
    return this;
  }
  
  
  public Type getReturnType()
  {
    if(returnType==null && !visitingStack.isEmpty()){
      returnType = visitingStack.pop();
    }
    return returnType;
  }
  
  public List<Type> getParameters()
  {
    return parameters;
  }
  
  

}
