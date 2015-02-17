package com.datatorrent.stram.webapp;

import java.util.ArrayList;
import java.util.List;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InnerClassNode;

public class ASMUtil
{

  public static boolean isEnum(ClassNode cn)
  {
    return (cn.access & Opcodes.ACC_ENUM) == Opcodes.ACC_ENUM;
  }

  public static ArrayList<String> getEnumValues(ClassNode cn)
  {
    return null;
//    for(InnerClassNode icn : cn.innerClasses){
//      
//    };
  }

}
