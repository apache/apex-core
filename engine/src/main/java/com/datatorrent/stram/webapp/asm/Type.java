/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;

import java.util.ArrayList;

import org.objectweb.asm.signature.SignatureVisitor;




public interface Type
{

  public static class TypeNode implements Type{
    
    private transient org.objectweb.asm.Type typeObj;
    
    private String objByteCode;
    
    public org.objectweb.asm.Type getTypeObj()
    {
      if(typeObj == null){
        typeObj = org.objectweb.asm.Type.getType(objByteCode);
      }
      return typeObj;
    }
    
    @Override
    public String toString()
    {
      if(typeObj == null){
        typeObj = org.objectweb.asm.Type.getType(objByteCode);
      }
      
      if(typeObj.getSort()==org.objectweb.asm.Type.OBJECT){
        return "class " + typeObj.getClassName();
      } else {
        return typeObj.getClassName();
      }
    }

    @Override
    public String getByteString()
    {
      if(typeObj == null){
        typeObj = org.objectweb.asm.Type.getType(objByteCode);
      }
      return "L" + typeObj.getClassName() + ";";
    }

    public String getObjByteCode()
    {
      return objByteCode;
    }

    public void setObjByteCode(String objByteCode)
    {
      this.objByteCode = objByteCode;
    }
    

  }
  
  public static class WildcardTypeNode implements Type{

    char boundChar;
    
    ArrayList<Type> bounds = new ArrayList<Type>();
    
    public Type[] getUpperBounds()
    {
      if(boundChar == SignatureVisitor.EXTENDS)
      {
        return bounds.toArray(new Type[]{});
      } else 
      {
        return null;
      }
    }

    public Type[] getLowerBounds()
    {
      if(boundChar == SignatureVisitor.SUPER)
      {
        return bounds.toArray(new Type[]{});
      } else 
      {
        return null;
      }
    }

    @Override
    public String getByteString()
    {
      return boundChar + "";
    }
    
    
    
  }
  
  public static class TypeVariableNode implements Type {

    String typeLiteral;
    
    ArrayList<Type> bounds = new ArrayList<Type>();
    
    @Override
    public String getByteString()
    {
      return bounds.get(0).getByteString();
    }
    
    public Type[] getBounds() {
      return bounds.toArray(new Type[]{});
    }
    
    public String getTypeLiteral()
    {
      return typeLiteral;
    }
    
    public TypeNode getRawTypeBound(){
      Type t = bounds.get(0);
      
      // The bounds can only be TypeNode or TypeVariableNode
      if(t instanceof TypeNode){
        return (TypeNode) t;
      }
      return ((TypeVariableNode)t).getRawTypeBound();
    }
    
  }
  
  public static class ParameterizedTypeNode extends TypeNode {
    
    ArrayList<Type> actualTypeArguments = new ArrayList<Type>();
    
    public Type[] getActualTypeArguments(){
      return actualTypeArguments.toArray(new Type[]{});
    }
  }
  
  
  public static class ArrayTypeNode implements Type {
    
    Type actualArrayType;
    
    public Type getActualArrayType(){
      return actualArrayType;
    }

    @Override
    public String getByteString()
    {
      return "[" + actualArrayType.getByteString();
    }
    
  }
  
  String getByteString();

}
