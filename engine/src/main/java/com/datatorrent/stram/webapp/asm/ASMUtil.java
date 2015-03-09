package com.datatorrent.stram.webapp.asm;

import java.util.LinkedList;
import java.util.List;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodNode;

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
    List<String> result = new LinkedList<String>();
    for(FieldNode fn : (List<FieldNode>)cn.fields){
      if(isEnumValue(fn))
        result.add(fn.name);
    };
    return result;
  }
  
  

  /**
   * Get a list of setter methods from classnode asmNode
   * @param asmNode
   * @return empty list if there is no setter method
   */
  public static List<MethodNode> getPublicSetter(ClassNode asmNode){
    List<MethodNode> result = new LinkedList<MethodNode>();
    @SuppressWarnings("unchecked")
    List<MethodNode> mList = asmNode.methods;
    for (MethodNode methodNode : mList) {

      if(methodNode.name.startsWith("set") && 
          isPublic(methodNode.access) && 
          Type.getArgumentTypes(methodNode.desc).length == 1 &&
          Type.getReturnType(methodNode.desc) == Type.VOID_TYPE)
        result.add(methodNode);
    }
    return result;
  }
  
  
  /**
   * Get a list of getter methods from classnode asmNode
   * @param asmNode
   * @return empty list if there is no getter method
   */
  public static List<MethodNode> getPublicGetter(ClassNode asmNode){
    List<MethodNode> result = new LinkedList<MethodNode>();
    @SuppressWarnings("unchecked")
    List<MethodNode> mList = asmNode.methods;
    for (MethodNode methodNode : mList) {

      if(
          //arg-list is 0 and return type is not void
          (Type.getArgumentTypes(methodNode.desc).length == 0 && 
          Type.getReturnType(methodNode.desc) != Type.VOID_TYPE) &&
          // and method name start with get or start with is if return type is boolean
          (methodNode.name.startsWith("get") || (methodNode.name.startsWith("is") && Type.getReturnType(methodNode.desc) == Type.BOOLEAN_TYPE))
          &&
          // and must be public
          isPublic(methodNode.access)  
          )
        result.add(methodNode);
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
      if(methodNode.name.equals(CONSTRUCTOR_NAME) && 
          isPublic(methodNode.access) && 
          Type.getArgumentTypes(methodNode.desc).length == 0)
        return methodNode;
    }
    return null;
  }



  private static boolean isPublic(int opCode)
  {
    return (opCode & Opcodes.ACC_PUBLIC) == Opcodes.ACC_PUBLIC;
  }
  
}
