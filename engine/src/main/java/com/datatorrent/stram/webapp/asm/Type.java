package com.datatorrent.stram.webapp.asm;

import java.util.ArrayList;

import org.objectweb.asm.signature.SignatureVisitor;




public interface Type
{

  public static class TypeNode implements Type{
    
    org.objectweb.asm.Type typeObj;
    
    public org.objectweb.asm.Type getTypeObj()
    {
      return typeObj;
    }
    
    @Override
    public String toString()
    {
      if(typeObj.getSort()==org.objectweb.asm.Type.OBJECT){
        return "class " + typeObj.getClassName();
      } else {
        return typeObj.getClassName();
      }
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
  }
  
  



}
