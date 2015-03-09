package com.datatorrent.stram.webapp.asm;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import org.objectweb.asm.signature.SignatureVisitor;

import com.datatorrent.stram.webapp.asm.Type.ArrayTypeNode;
import com.datatorrent.stram.webapp.asm.Type.ParameterizedTypeNode;
import com.datatorrent.stram.webapp.asm.Type.TypeNode;
import com.datatorrent.stram.webapp.asm.Type.WildcardTypeNode;

/**
 * Follow the visiting path of ASM
 * to visit getter and setter method signature
 * 
 * ClassSignature = ( visitFormalTypeParameter visitClassBound? visitInterfaceBound* )* ( visitSuperClass visitInterface* )
 * MethodSignature = ( visitFormalTypeParameter visitClassBound? visitInterfaceBound* )* ( visitParameterType* visitReturnType visitExceptionType* )
 * TypeSignature = visitBaseType | visitTypeVariable | visitArrayType | ( visitClassType visitTypeArgument* ( visitInnerClassType visitTypeArgument* )* visitEnd ) )
 * 
 */
public class MethodSignatureVisitor implements SignatureVisitor
{
  
  // There is at most 1 parameter for setter and getter method
  private List<Type> parameters = new LinkedList<Type>();
  
  private Type returnType;
  
  private List<Type> exceptionType = new LinkedList<Type>();
  
  private int stage = -1;
  
  private static final int VISIT_PARAM = 0;
  
  private static final int VISIT_RETURN = 1;
  
  private static final int VISIT_EXCEPTION = 2;
  
  private Stack<Type> visitingStack = new Stack<Type>();


  
  @Override
  public SignatureVisitor visitArrayType()
  {
    //System.out.println("visitArrayType　");
    ArrayTypeNode at = new ArrayTypeNode();
    visitingStack.push(at);
    return this;
  }

  @Override
  public void visitBaseType(char baseType)
  {
    
    Type.TypeNode tn = new Type.TypeNode();
    tn.typeObj = org.objectweb.asm.Type.getType(baseType + "");
//    System.out.println(tn.typeObj);
    visitingStack.push(tn);
    resolveStack();
    // base type could only appear in method parameter list or return type  
//    if(stage == VISIT_PARAM) {
//      visitingStack.push(tn);
//    }
//    if(stage == VISIT_RETURN) {
//      returnType = tn;
//    }
    //System.out.println("visitBaseType:'" + baseType);
  }


  @Override
  public void visitClassType(String classType)
  { 
    Type.TypeNode tn = new Type.TypeNode();
    tn.typeObj = org.objectweb.asm.Type.getType("L" + classType + ";");
//    System.out.println(tn.typeObj);
    visitingStack.push(tn);
    // base type could only appear in method parameter list or return type  
//    if(stage == VISIT_PARAM) {
//      visitingStack.push(tn);
//    }
//    if(stage == VISIT_RETURN) {
//      returnType = tn;
//    } if(stage == VISIT_EXCEPTION) {
//      exceptionType = tn;
//    } 
    //System.out.print("visitClassType:'" + classType + "'  ,  ");
  }
  
  private void resolveStack() {
    if(visitingStack.isEmpty() || visitingStack.size()==1){
      return;
    }
    Type top = visitingStack.pop();
    Type peek = visitingStack.peek();
    
    if(peek instanceof ParameterizedTypeNode){
      ((ParameterizedTypeNode)peek).actualTypeArguments.add(top);
      return;
    } else if(peek instanceof ArrayTypeNode) {
      ((ArrayTypeNode)peek).actualArrayType = top;
      resolveStack();
    } else if(peek instanceof WildcardTypeNode) {
      ((WildcardTypeNode)peek).bounds.add(top);
      resolveStack();
    } else {
      visitingStack.push(top);
      return;
    }
    
  }

  @Override
  public void visitEnd()
  {
    resolveStack();
    //System.out.print("visitEnd　");
    //System.out.println();
  }

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
  public void visitInnerClassType(String classType)
  {
    visitClassType(classType); 
    //System.out.print("visitInnerClassType:'" + classType + "'  ,  ");
  }



  @Override
  public SignatureVisitor visitParameterType()
  {
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


  @Override
  public void visitTypeArgument()
  { 

    
  }

  @Override
  public SignatureVisitor visitTypeArgument(char typeArg)
  {
    TypeNode t = (TypeNode) visitingStack.pop();
    if (t instanceof ParameterizedTypeNode) {
      visitingStack.push(t);
    } else {
      ParameterizedTypeNode pt = new ParameterizedTypeNode();
      pt.typeObj = t.typeObj;
      visitingStack.push(pt);
    }
    
    if(typeArg == SignatureVisitor.INSTANCEOF){
      return this;
    }        
    WildcardTypeNode wtn = new WildcardTypeNode();
    wtn.boundChar = typeArg;
    visitingStack.push(wtn);
    
    //System.out.print("visitTypeArgument:'" + typeArg + "'  ,  ");
    return this;
  }
  
  

  @Override
  public void visitTypeVariable(String typeVariable)
  {
    TypeNode tn = new TypeNode();
    tn.typeObj = org.objectweb.asm.Type.getType("T" + typeVariable + ";");
    System.out.println(tn.typeObj);
    visitingStack.push(tn);
    resolveStack();
    //System.out.println("visitTypeVariable:'" + typeVariable);
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
  
  @Override
  public SignatureVisitor visitInterface()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SignatureVisitor visitInterfaceBound()
  {
//    System.out.println("visitInterfaceBound");
    return this;
  }

  @Override
  public SignatureVisitor visitSuperclass()
  {
    
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void visitFormalTypeParameter(String typeVariable)
  {
//    System.out.println("visitFormalTypeParameter");

//    throw new UnsupportedOperationException();
  }

  @Override
  public SignatureVisitor visitClassBound()
  {
//    System.out.println("visitClassBound");
    return this;
    //throw new UnsupportedOperationException();
  }

  
}
