/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp.asm;

import java.util.List;

import org.objectweb.asm.Opcodes;

public class CompactClassNode
{
  
  private int access;
  
  private String name;
  
  private List<CompactMethodNode> getterMethods;
  
  private List<CompactMethodNode> setterMethods;
  
  private CompactMethodNode initializableConstructor;
  
  private List<CompactClassNode> innerClasses;
  
  private List<String> enumValues;
  
  public int getAccess()
  {
    return access;
  }

  public void setAccess(int access)
  {
    this.access = access;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public CompactMethodNode getInitializableConstructor()
  {
    return initializableConstructor;
  }

  public void setInitializableConstructor(CompactMethodNode initializableConstructor)
  {
    this.initializableConstructor = initializableConstructor;
  }

  public List<CompactClassNode> getInnerClasses()
  {
    return innerClasses;
  }

  public void setInnerClasses(List<CompactClassNode> innerClasses)
  {
    this.innerClasses = innerClasses;
  }

  public List<String> getEnumValues()
  {
    return enumValues;
  }

  public void setEnumValues(List<String> enumValues)
  {
    this.enumValues = enumValues;
  }

  public boolean isEnum()
  {
    return (access & Opcodes.ACC_ENUM) == Opcodes.ACC_ENUM;
  }

  public List<CompactMethodNode> getGetterMethods()
  {
    return getterMethods;
  }

  public void setGetterMethods(List<CompactMethodNode> getterMethods)
  {
    this.getterMethods = getterMethods;
  }

  public List<CompactMethodNode> getSetterMethods()
  {
    return setterMethods;
  }

  public void setSetterMethods(List<CompactMethodNode> setterMethods)
  {
    this.setterMethods = setterMethods;
  }

}
