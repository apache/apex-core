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

import java.util.ArrayList;

import org.apache.xbean.asm5.signature.SignatureVisitor;

/**
 * Data structure for java type
 *
 * @since 2.1
 */
public interface Type
{

  public static class TypeNode implements Type{
    
    private transient org.apache.xbean.asm5.Type typeObj;
    
    private String objByteCode;
    
    public org.apache.xbean.asm5.Type getTypeObj()
    {
      if(typeObj == null){
        typeObj = org.apache.xbean.asm5.Type.getType(objByteCode);
      }
      return typeObj;
    }
    
    @Override
    public String toString()
    {
      if(typeObj == null){
        typeObj = org.apache.xbean.asm5.Type.getType(objByteCode);
      }
      
      if(typeObj.getSort()==org.apache.xbean.asm5.Type.OBJECT){
        return "class " + typeObj.getClassName();
      } else {
        return typeObj.getClassName();
      }
    }

    @Override
    public String getByteString()
    {
      if(typeObj == null){
        typeObj = org.apache.xbean.asm5.Type.getType(objByteCode);
      }
      if (typeObj.getSort() == org.apache.xbean.asm5.Type.OBJECT) {
        return "L" + typeObj.getClassName() + ";";
      } else {
        return typeObj.toString();
      }
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
