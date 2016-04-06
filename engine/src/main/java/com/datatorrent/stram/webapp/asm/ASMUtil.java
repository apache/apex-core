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

import org.apache.xbean.asm5.Opcodes;
import org.apache.xbean.asm5.Type;
import org.apache.xbean.asm5.tree.ClassNode;
import org.apache.xbean.asm5.tree.FieldNode;
import org.apache.xbean.asm5.tree.MethodNode;

/**
 * Provide util function to deal with java bytecode
 *
 * @since 2.1
 */
public class ASMUtil
{
  public static final String CONSTRUCTOR_NAME = "<init>";

  public static boolean isEnum(ClassNode cn)
  {
    return (cn.access & Opcodes.ACC_ENUM) == Opcodes.ACC_ENUM;
  }

  public static boolean isEnumValue(FieldNode fn)
  {
    return (fn.access & Opcodes.ACC_ENUM) == Opcodes.ACC_ENUM;
  }

  @SuppressWarnings("unchecked")
  public static List<String> getEnumValues(ClassNode cn)
  {
    List<String> result = new LinkedList<>();
    for (FieldNode fn : (List<FieldNode>)cn.fields) {
      if (isEnumValue(fn)) {
        result.add(fn.name);
      }
    }
    return result;
  }

  public static List<FieldNode> getPorts(ClassNode asmNode)
  {
    List<FieldNode> result = new LinkedList<>();
    List<FieldNode> fields = asmNode.fields;
    for (FieldNode fn : fields) {
      // 'L' represents <class>, ignore primitive types
      if (fn.desc.charAt(0) == 'L') {
        result.add(fn);
      }
    }

    return result;
  }

  /**
   * Get a list of setter methods from classnode asmNode
   * @param asmNode
   * @return empty list if there is no setter method
   */
  public static List<MethodNode> getPublicSetter(ClassNode asmNode)
  {
    List<MethodNode> result = new LinkedList<>();
    @SuppressWarnings("unchecked")
    List<MethodNode> mList = asmNode.methods;
    for (MethodNode methodNode : mList) {

      if (methodNode.name.startsWith("set") &&
          isPublic(methodNode.access) &&
          Type.getArgumentTypes(methodNode.desc).length == 1 &&
          Type.getReturnType(methodNode.desc) == Type.VOID_TYPE) {
        result.add(methodNode);
      }
    }
    return result;
  }

  /**
   * Get a list of getter methods from classnode asmNode
   * @param asmNode
   * @return empty list if there is no getter method
   */
  public static List<MethodNode> getPublicGetter(ClassNode asmNode)
  {
    List<MethodNode> result = new LinkedList<>();
    @SuppressWarnings("unchecked")
    List<MethodNode> mList = asmNode.methods;
    for (MethodNode methodNode : mList) {
      // arg-list is 0 and return type is not void
      // and method name start with get or start with is if return type is boolean
      // and must be public
      if ((Type.getArgumentTypes(methodNode.desc).length == 0 && Type.getReturnType(methodNode.desc) != Type.VOID_TYPE) &&
          (methodNode.name.startsWith("get") || (methodNode.name.startsWith("is") && Type.getReturnType(methodNode.desc) == Type.BOOLEAN_TYPE)) &&
          isPublic(methodNode.access)) {
        result.add(methodNode);
      }
    }
    return result;
  }

  /**
   * Get all public default constructor(0-arg) for the classnode
   *
   * @param asmNode
   * @return null if there is no public default constructor
   */
  public static MethodNode getPublicDefaultConstructor(ClassNode asmNode)
  {
    @SuppressWarnings("unchecked")
    List<MethodNode> mList = asmNode.methods;
    for (MethodNode methodNode : mList) {
      if (methodNode.name.equals(CONSTRUCTOR_NAME) &&
          isPublic(methodNode.access) &&
          Type.getArgumentTypes(methodNode.desc).length == 0) {
        return methodNode;
      }
    }
    return null;
  }

  public static boolean isPublic(int opCode)
  {
    return (opCode & Opcodes.ACC_PUBLIC) == Opcodes.ACC_PUBLIC;
  }

  public static boolean isAbstract(int opCode)
  {
    return (opCode & Opcodes.ACC_ABSTRACT) == Opcodes.ACC_ABSTRACT;
  }

  public static boolean isTransient(int opCode)
  {
    return (opCode & Opcodes.ACC_TRANSIENT) == Opcodes.ACC_TRANSIENT;
  }

  public static boolean isFinal(int opCode)
  {
    return (opCode & Opcodes.ACC_FINAL) == Opcodes.ACC_FINAL;
  }
}
