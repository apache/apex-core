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

import java.util.LinkedList;
import java.util.List;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InnerClassNode;
import org.objectweb.asm.tree.MethodNode;


/**
 * A util class extract only data needed in app builder
 *
 * @since 2.1
 */
public class CompactUtil
{

  public static CompactClassNode compactClassNode(ClassNode cn)
  {
    if(cn == null){
      return null;
    }
    CompactClassNode ccn = new CompactClassNode();
    ccn.setAccess(cn.access);
    ccn.setInitializableConstructor(compactMethodNode(ASMUtil.getPublicDefaultConstructor(cn)));
    
    List<CompactMethodNode> cmns = new LinkedList<CompactMethodNode>();
    for (MethodNode mn : ASMUtil.getPublicGetter(cn)) {
      cmns.add(compactMethodNode(mn));
    }
    ccn.setGetterMethods(cmns);
    
    cmns = new LinkedList<CompactMethodNode>();
    for (MethodNode mn : ASMUtil.getPublicSetter(cn)) {
      cmns.add(compactMethodNode(mn));
    }
    ccn.setSetterMethods(cmns);
   
        
    ccn.setName(cn.name);
    
    List<CompactClassNode> ccns = new LinkedList<CompactClassNode>();
    for (Object icn : cn.innerClasses) {
      CompactClassNode inner = new CompactClassNode();
      inner.setName(((InnerClassNode)icn).name);
      inner.setAccess(((InnerClassNode)icn).access);
    }
    ccn.setInnerClasses(ccns);
    if(ASMUtil.isEnum(cn)){
      ccn.setEnumValues(ASMUtil.getEnumValues(cn));
    }
    
    if(cn instanceof ClassNodeType){
      ccn.setCsv(((ClassNodeType)cn).csv);
    }
    
    
//    if(!CollectionUtils.isEmpty(cn.innerClasses)){
//      ccn.setInnerClasses(Lists.transform(cn.innerClasses, new Function<InnerClassNode, CompactClassNode>(){
//
//        @Override
//        public CompactClassNode apply(InnerClassNode input)
//        {
//          input.
//          return null;
//        }
//        
//      }));
//    }
    return ccn;
  }

  private static CompactMethodNode compactMethodNode(MethodNode mn)
  {
    if (mn == null) {
      return null;
    }
    CompactMethodNode cmn = new CompactMethodNode();
    cmn.setName(mn.name);
    if(mn instanceof com.datatorrent.stram.webapp.asm.MethodNode)
    cmn.setMethodSignatureNode(((com.datatorrent.stram.webapp.asm.MethodNode)mn).signatureNode);
    return cmn;
  }

}
