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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.xbean.asm5.tree.AnnotationNode;
import org.apache.xbean.asm5.tree.ClassNode;
import org.apache.xbean.asm5.tree.FieldNode;
import org.apache.xbean.asm5.tree.InnerClassNode;
import org.apache.xbean.asm5.tree.MethodNode;


/**
 * A util class extract only data needed in app builder
 *
 * @since 2.1
 */
public class CompactUtil
{

  public static CompactClassNode compactClassNode(ClassNode cn)
  {
    if (cn == null) {
      return null;
    }
    CompactClassNode ccn = new CompactClassNode();
    ccn.setAccess(cn.access);
    ccn.setDefaultConstructor(compactMethodNode(ASMUtil.getPublicDefaultConstructor(cn)));

    List<CompactMethodNode> cmns = new LinkedList<>();
    for (MethodNode mn : ASMUtil.getPublicGetter(cn)) {
      cmns.add(compactMethodNode(mn));
    }
    ccn.setGetterMethods(cmns);

    cmns = new LinkedList<>();
    for (MethodNode mn : ASMUtil.getPublicSetter(cn)) {
      cmns.add(compactMethodNode(mn));
    }
    ccn.setSetterMethods(cmns);

    ccn.setPorts(new LinkedList<CompactFieldNode>());
    ccn.setName(cn.name);

    List<CompactClassNode> ccns = new LinkedList<>();
    for (Object icn : cn.innerClasses) {
      CompactClassNode inner = new CompactClassNode();
      inner.setName(((InnerClassNode)icn).name);
      inner.setAccess(((InnerClassNode)icn).access);
    }
    ccn.setInnerClasses(ccns);
    if (ASMUtil.isEnum(cn)) {
      ccn.setEnumValues(ASMUtil.getEnumValues(cn));
    }

    if (cn instanceof ClassNodeType) {
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

  public static void updateCompactClassPortInfo(ClassNode cn, CompactClassNode ccn)
  {
    List<FieldNode> fields = ASMUtil.getPorts(cn);
    List<CompactFieldNode> ports = new LinkedList<>();
    for (FieldNode fn : fields) {
      ports.add(compactFieldNode(fn));
    }
    ccn.setPorts(ports);
  }

  private static CompactMethodNode compactMethodNode(MethodNode mn)
  {
    if (mn == null) {
      return null;
    }
    CompactMethodNode cmn = new CompactMethodNode();
    cmn.setName(mn.name);
    if (mn instanceof com.datatorrent.stram.webapp.asm.MethodNode) {
      cmn.setMethodSignatureNode(((com.datatorrent.stram.webapp.asm.MethodNode)mn).signatureNode);
    }
    return cmn;
  }

  private static CompactFieldNode compactFieldNode(FieldNode fn)
  {
    if (fn == null) {
      return null;
    }
    CompactFieldNode cfn = new CompactFieldNode();
    cfn.setName(fn.name);

    String className = org.apache.xbean.asm5.Type.getObjectType(fn.desc).getClassName();
    if (className.charAt(0) == 'L') {
      className = className.substring(1);
    }
    if (className.endsWith(";")) {
      className = className.substring(0, className.length() - 1);
    }
    cfn.setDescription(className);
    cfn.setSignature(fn.signature);

    if (fn.visibleAnnotations != null) {
      setAnnotationNode(fn, cfn);
    }
    if (fn instanceof com.datatorrent.stram.webapp.asm.FieldNode) {
      cfn.setFieldSignatureNode((((com.datatorrent.stram.webapp.asm.FieldNode)fn).signatureNode));
    }
    return cfn;
  }

  private static void setAnnotationNode(FieldNode fn, CompactFieldNode cfn)
  {
    List<CompactAnnotationNode> annotations = new LinkedList<>();
    for (Object visibleAnnotation : fn.visibleAnnotations) {
      CompactAnnotationNode node = new CompactAnnotationNode();
      Map<String, Object> annotationMap = new HashMap<>();
      if (visibleAnnotation instanceof AnnotationNode) {
        AnnotationNode annotation = (AnnotationNode)visibleAnnotation;
        if (annotation.desc.contains("InputPortFieldAnnotation")
            || annotation.desc.contains("OutputPortFieldAnnotation")) {
          List<Object> annotationValues = annotation.values;
          if (annotationValues != null) {
            int index = 0;
            while (index <= annotationValues.size() - 2) {
              String key = (String)annotationValues.get(index++);
              Object value = annotationValues.get(index++);
              annotationMap.put(key, value);
            }
            node.setAnnotations(annotationMap);
            annotations.add(node);
          }
        }
      }
      cfn.setVisibleAnnotations(annotations);
    }
  }
}
