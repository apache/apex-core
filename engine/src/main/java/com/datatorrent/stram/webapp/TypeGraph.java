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
package com.datatorrent.stram.webapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.codehaus.jackson.map.deser.std.FromStringDeserializer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Component;
import com.datatorrent.api.Operator;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.stram.webapp.asm.ClassNodeType;
import com.datatorrent.stram.webapp.asm.ClassSignatureVisitor;
import com.datatorrent.stram.webapp.asm.CompactClassNode;
import com.datatorrent.stram.webapp.asm.CompactFieldNode;
import com.datatorrent.stram.webapp.asm.CompactMethodNode;
import com.datatorrent.stram.webapp.asm.CompactUtil;
import com.datatorrent.stram.webapp.asm.MethodSignatureVisitor;
import com.datatorrent.stram.webapp.asm.Type;
import com.datatorrent.stram.webapp.asm.Type.ArrayTypeNode;
import com.datatorrent.stram.webapp.asm.Type.ParameterizedTypeNode;
import com.datatorrent.stram.webapp.asm.Type.TypeNode;
import com.datatorrent.stram.webapp.asm.Type.TypeVariableNode;
import com.datatorrent.stram.webapp.asm.Type.WildcardTypeNode;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.primitives.Primitives;

/**
 * A graph data structure holds all type information and their relationship needed in app builder
 * ASM is used to retrieve fields, methods information and kept in java beans and then stored in this graph data structure
 * ASM is used to avoid memory leak and save permgen memory space
 *
 * @since 2.1
 */
@SuppressWarnings("unchecked")
public class TypeGraph
{
  // classes to exclude when fetching getter/setter method or in other parsers
  public static final String[] EXCLUDE_CLASSES = {Object.class.getName().replace('.', '/'), 
    Enum.class.getName().replace('.', '/'), 
    Operator.class.getName().replace('.', '/'),
    Component.class.getName().replace('.', '/')};
  
  
  public static final HashMap<String, String> classReplacement;
  
  static {
    classReplacement = new HashMap<String, String>();
    for (@SuppressWarnings("rawtypes") FromStringDeserializer fsd : FromStringDeserializer.all()) {
      classReplacement.put(fsd.getValueClass().getName(), "java.lang.String");
    }
    for (@SuppressWarnings("rawtypes") Class wrapperClass : Primitives.allWrapperTypes()) {
      classReplacement.put(wrapperClass.getName(), Primitives.unwrap(wrapperClass).getName());
    }
  }

  private static boolean isAncestor(TypeGraphVertex typeTgv, TypeGraphVertex tgv)
  {
    if (tgv == typeTgv) {
      return true;
    }
    if ((tgv.ancestors == null || tgv.ancestors.size() == 0)) {
      return false;
    }
    for (TypeGraphVertex vertex : tgv.ancestors) {
      if (isAncestor(typeTgv, vertex))
        return true;
    }
    return false;
  }
  
  enum UI_TYPE {

    LIST(Collection.class.getName(), "List"),

    ENUM(Enum.class.getName(), "Enum"),

    MAP(Map.class.getName(), "Map");

    private final String assignableTo;
    private final String name;

    private UI_TYPE(String assignableTo, String name)
    {
      this.assignableTo = assignableTo;
      this.name = name;
    }

    public static UI_TYPE getEnumFor(String clazzName, Map<String, TypeGraphVertex> typeGraph)
    {
      TypeGraphVertex tgv = typeGraph.get(clazzName);
      if (tgv == null) {
        return null;
      }
      for (UI_TYPE type : UI_TYPE.values()) {
        TypeGraphVertex typeTgv = typeGraph.get(type.assignableTo);
        if (typeTgv == null) {
          continue;
        }
        if (isAncestor(typeTgv, tgv)) {
          return type;
        }
      }
      return null;
    }

    

    public String getName()
    {
      return name;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TypeGraph.class);

  private final Map<String, TypeGraphVertex> typeGraph = new HashMap<String, TypeGraphVertex>();

  private void addNode(InputStream input, String resName) throws IOException
  {
    try {
      
      ClassReader reader = new ClassReader(input);
      ClassNode classN = new ClassNodeType();
      reader.accept(classN, ClassReader.SKIP_CODE);
      CompactClassNode ccn = CompactUtil.compactClassNode(classN);
      String typeName = classN.name.replace('/', '.');
      
      TypeGraphVertex tgv = null;
      TypeGraphVertex ptgv = null;
      if (typeGraph.containsKey(typeName)) {
        tgv = typeGraph.get(typeName);
        tgv.setClassNode(ccn);
        tgv.setJarName(resName); // If tgv was already populated for superclass/interface, jar name needs to be updated 
      } else {
        tgv = new TypeGraphVertex(typeName, resName, ccn);
        typeGraph.put(typeName, tgv);
      }
      String immediateP = reader.getSuperName();
      if (immediateP != null) {
        immediateP = immediateP.replace('/', '.');
        ptgv = typeGraph.get(immediateP);
        if (ptgv == null) {
          ptgv = new TypeGraphVertex(immediateP, resName);
          typeGraph.put(immediateP, ptgv);
        }
        tgv.ancestors.add(ptgv);
        ptgv.descendants.add(tgv);
      }
      if (reader.getInterfaces() != null) {
        for (String iface : reader.getInterfaces()) {
          iface = iface.replace('/', '.');
          ptgv = typeGraph.get(iface);
          if (ptgv == null) {
            ptgv = new TypeGraphVertex(iface, resName);
            typeGraph.put(iface, ptgv);
          }
          tgv.ancestors.add(ptgv);
          ptgv.descendants.add(tgv);
        }
      }

      updateInitializableDescendants(tgv);
    } finally {
      if (input != null) {
        input.close();
      }
    }
  }

  public void addNode(File file) throws IOException
  {
    addNode(new FileInputStream(file), file.getAbsolutePath());
  }

  public void addNode(JarEntry jarEntry, JarFile jar) throws IOException
  {
    addNode(jar.getInputStream(jarEntry), jar.getName());
  }

  public void updatePortTypeInfoInTypeGraph(Map<String, JarFile> openJarFiles,
      Map<String, File> openClassFiles) {
    TypeGraphVertex tgv = typeGraph.get(Operator.class.getName());
    updatePortTypeInfoInTypeGraph(openJarFiles, openClassFiles, tgv);
  }

  public void updatePortTypeInfoInTypeGraph(Map<String, JarFile> openJarFiles,
      Map<String, File> openClassFiles, TypeGraphVertex tgv) {
    if (tgv == null)
      return;

    for (TypeGraphVertex operator : tgv.descendants) {
      try {
        String path = operator.getJarName();
        JarFile jar = openJarFiles.get(path);
        if (jar != null) {
          String jarEntryName = operator.getClassNode().getName()
              .replace('.', '/')
              + ".class";
          JarEntry jarEntry = jar.getJarEntry(jarEntryName);
          if (jarEntry != null) {
            updatePortInfo(operator, jar.getInputStream(jarEntry));
          }
        } else {
          File f = openClassFiles.get(path);
          if (f != null && f.exists() && f.getName().endsWith("class")) {
            updatePortInfo(operator, new FileInputStream(f));
          }
        }
        updatePortTypeInfoInTypeGraph(openJarFiles, openClassFiles, operator);
      } catch (Exception e) {
        DTThrowable.wrapIfChecked(e);
      }
    }
  }

  private void updatePortInfo(TypeGraphVertex tgv, InputStream input)
      throws IOException {
    try {
      ClassReader reader;
      reader = new ClassReader(input);
      ClassNodeType classN = new ClassNodeType();
      classN.setClassSignatureVisitor(tgv.getClassNode().getCsv());
      classN.setVisitFields(true);
      reader.accept(classN, ClassReader.SKIP_CODE);
      CompactClassNode ccn = tgv.getClassNode();
      CompactUtil.updateCompactClassPortInfo(classN, ccn);
      List<CompactFieldNode> prunedFields = new LinkedList<CompactFieldNode>();
      TypeGraphVertex portVertex = typeGraph.get(Operator.Port.class.getName());
      for (CompactFieldNode field : ccn.getPorts()) {
        TypeGraphVertex fieldVertex = typeGraph.get(field.getDescription());
        if(fieldVertex != null) {
          if (isAncestor(portVertex, fieldVertex)) {
            prunedFields.add(field);
          }
        }
      }
      ccn.setPorts(prunedFields);

    } finally {
      if (input != null) {
        input.close();
      }
    }
  }

  private void updateInitializableDescendants(TypeGraphVertex tgv)
  {
    if(tgv.isInitializable()){
      tgv.allInitialiazableDescendants.add(tgv);
    }
    for (TypeGraphVertex parent : tgv.ancestors) {
      updateInitializableDescendants(parent, tgv.allInitialiazableDescendants);
    }
  }

  private void updateInitializableDescendants(TypeGraphVertex tgv, Set<TypeGraphVertex> allChildren)
  {

    tgv.allInitialiazableDescendants.addAll(allChildren);


    for (TypeGraphVertex parent : tgv.ancestors) {
      updateInitializableDescendants(parent, allChildren);
    }
  }

  public int size()
  {
    return typeGraph.size();
  }

  public Set<String> getDescendants(String fullClassName)
  {
    Set<String> result = new HashSet<String>();
    TypeGraphVertex tgv = typeGraph.get(fullClassName);
    if (tgv != null) {
      tranverse(tgv, false, result, Integer.MAX_VALUE);
    }
    return result;
  }

  public List<String> getInitializableDescendants(String fullClassName)
  {
    return getInitializableDescendants(fullClassName, null, null, null);
  }

  private void tranverse(TypeGraphVertex tgv, boolean onlyInitializable, Set<String> result, int limit)
  {
    if (!onlyInitializable) {
      result.add(tgv.typeName);
    }

    if (onlyInitializable && tgv.numberOfInitializableDescendants() > limit) {
      throw new RuntimeException("Too many public concrete sub types!");
    }

    if (onlyInitializable && tgv.isInitializable()) {
      result.add(tgv.typeName);
    }

    if (tgv.descendants.size() > 0) {
      for (TypeGraphVertex child : tgv.descendants) {
        tranverse(child, onlyInitializable, result, limit);
      }
    }
  }

  public static class TypeGraphVertex
  {

    /**
     * Vertex is unique by name, hashCode and equal depends only on typeName
     */
    public final String typeName;

    private CompactClassNode classNode = null;

    /**
     * All initializable(public type with a public non-arg constructor) implementations including direct and indirect descendants
     */
    private final transient SortedSet<TypeGraphVertex> allInitialiazableDescendants = new TreeSet<TypeGraphVertex>(new Comparator<TypeGraphVertex>() {

      @Override
      public int compare(TypeGraphVertex o1, TypeGraphVertex o2)
      {
        String n1 = o1.typeName;
        String n2 = o2.typeName;
        if(n1.startsWith("java")){
          n1 = "0" + n1;
        }
        if(n2.startsWith("java")){
          n2 = "0" + n2;
        }
        
        return n1.compareTo(n2);
      }
    });

    private final transient Set<TypeGraphVertex> ancestors = new HashSet<TypeGraphVertex>();

    private final transient Set<TypeGraphVertex> descendants = new HashSet<TypeGraphVertex>();

    // keep the jar file name for late fetching the detail information
    private String jarName;
    
    @SuppressWarnings("unused")
    private TypeGraphVertex(){
      jarName = "";
      typeName = "";
    }

    public TypeGraphVertex(String typeName, String jarName, CompactClassNode classNode)
    {

      this.jarName = jarName;
      this.typeName = typeName;
      this.classNode = classNode;
    }

    public int numberOfInitializableDescendants()
    {
      return allInitialiazableDescendants.size() + (isInitializable() ? 1 : 0);
    }

    public TypeGraphVertex(String typeName, String jarName)
    {
      this.typeName = typeName;
      this.jarName = jarName;
    }

    private boolean isInitializable()
    {
      return isPublicConcrete() && classNode.getInitializableConstructor() != null;
    }

    private boolean isPublicConcrete()
    {
      if (classNode == null) {
        // If the class is not in the classpath
        return false;
      }
      int opCode = getOpCode();

      // if the class is neither abstract nor interface
      // and the class is public
      return ((opCode & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_INTERFACE)) == 0) && ((opCode & Opcodes.ACC_PUBLIC) == Opcodes.ACC_PUBLIC);
    }

    private int getOpCode()
    {
      List<CompactClassNode> icl = classNode.getInnerClasses();
      if (typeName.contains("$")) {
        for (CompactClassNode innerClassNode : icl) {
          if (innerClassNode.getName().replace('/', '.').equals(typeName)) {
            return innerClassNode.getAccess();
          }
        }
      }
      return classNode.getAccess();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((typeName == null) ? 0 : typeName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TypeGraphVertex other = (TypeGraphVertex) obj;
      if (typeName == null) {
        if (other.typeName != null)
          return false;
      } else if (!typeName.equals(other.typeName))
        return false;
      return true;
    }

    public String getJarName()
    {
      return jarName;
    }

    public void setJarName(String jarName)
    {
      this.jarName =  jarName;
    }

    public CompactClassNode getClassNode()
    {
      return classNode;
    }
    
    public void setClassNode(CompactClassNode classNode)
    {
      this.classNode = classNode;
    }
  }

  /**
   * @param clazz parent class
   * @param filter
   * @param packagePrefix 
   * @param startsWith  case insensitive
   * @return all initializable descendants of class clazz which comfort to filter expression, packagePrefix and start with $startsWith
   */
  public List<String> getInitializableDescendants(String clazz, String filter, String packagePrefix, String startsWith)
  {
    TypeGraphVertex tgv = typeGraph.get(clazz);
    if(tgv == null) {
      return null;
    }
    
    List<String> result = new LinkedList<String>();
    if (tgv != null) {
      
      for (TypeGraphVertex node : tgv.allInitialiazableDescendants) {
        String typeName = node.typeName;
        if (filter != null && !Pattern.matches(filter, node.typeName)) {
          continue;
        }
        if (packagePrefix != null && !node.typeName.startsWith(packagePrefix)) {
          continue;
        }
        if (startsWith != null && !typeName.substring(typeName.lastIndexOf('.') + 1).toLowerCase().startsWith(startsWith.toLowerCase())){
          continue;
        }
        result.add(node.typeName);
      }
    }
    return result;
  }

  public JSONObject describeClass(String clazzName) throws JSONException
  {
    JSONObject desc = new JSONObject();
    desc.put("name", clazzName);
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    if (tgv == null) {
      return desc;
    }
    CompactClassNode cn = tgv.classNode;
    if (cn.isEnum()) {

      List<String> enumNames = cn.getEnumValues();
      desc.put("enum", enumNames);
      desc.put("uiType", UI_TYPE.ENUM.getName());
    }
    UI_TYPE uType = UI_TYPE.getEnumFor(tgv.typeName, typeGraph);
    if (uType != null) {
      desc.put("uiType", uType.getName());
    }
    
    addClassPropertiesAndPorts(clazzName,  desc);

    return desc;
  }

  private Collection<JSONObject> getPortTypeInfo(String clazzName,
      Map<Type, Type> typeReplacement, List<CompactFieldNode> ports) throws JSONException {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    if (tgv == null) {
      return null;
    }
    
    Collection<JSONObject> portInfo = new ArrayList<JSONObject>();
    
    for (CompactFieldNode port : ports) {
        Type fieldType = port.getFieldSignatureNode().getFieldType();
        Type t = fieldType;
        if (fieldType instanceof ParameterizedTypeNode) {
          // TODO: Right now getPortInfo assumes a single parameterized type
          t = ((ParameterizedTypeNode) fieldType).getActualTypeArguments()[0];
        } else {
          // TODO: Check behavior for Ports not using Default Input/output ports
          TypeGraphVertex portVertex = typeGraph.get(port.getDescription());
          t = findTypeArgument(portVertex, typeReplacement);
          LOG.debug("Field is of type {}", fieldType.getClass());
        }

        JSONObject meta = new JSONObject();
        try {
          meta.put("name", port.getName()); 
          setTypes(meta, t, typeReplacement);
          portInfo.add(meta);
        } catch (Exception e) {
          DTThrowable.wrapIfChecked(e);
        }
    }

    return portInfo;
  }

  public static Type getParameterizedTypeArgument(Type type) {
    if (type instanceof ParameterizedTypeNode) {
      return ((ParameterizedTypeNode) type).getActualTypeArguments()[0];
    }
    return null;
  }

  private Type findTypeArgument(TypeGraphVertex tgv,
      Map<Type, Type> typeReplacement) {
    if (tgv == null)
      return null;
    ClassSignatureVisitor csv = tgv.getClassNode().getCsv();
    Type superC = csv.getSuperClass();

    addReplacement(superC, typeReplacement);
    Type t = getParameterizedTypeArgument(superC);
    if (t != null) {
      return t;
    }

    if (csv.getInterfaces() != null) {
      for (Type it : csv.getInterfaces()) {
        addReplacement(it, typeReplacement);
        t = getParameterizedTypeArgument(it);
        if (t != null) {
          return t;
        }
      }
    }

    for (TypeGraphVertex ancestor : tgv.ancestors) {
      return findTypeArgument(ancestor, typeReplacement);
    }

    return null;
  }

  public List<CompactFieldNode> getAllInputPorts(String clazzName) {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    List<CompactFieldNode> ports = new ArrayList<CompactFieldNode>();
    TypeGraphVertex portVertex = typeGraph.get(Operator.InputPort.class
        .getName());
    getAllPortsWithAncestor(portVertex, tgv, ports);
    Collections.sort(ports, new Comparator<CompactFieldNode>() {
      @Override
      public int compare(CompactFieldNode a, CompactFieldNode b) {
        return a.getName().compareTo(b.getName());
      }
    });
    return ports;
  }

  public List<CompactFieldNode> getAllOutputPorts(String clazzName) {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    List<CompactFieldNode> ports = new ArrayList<CompactFieldNode>();
    TypeGraphVertex portVertex = typeGraph.get(Operator.OutputPort.class
        .getName());
    getAllPortsWithAncestor(portVertex, tgv, ports);
    Collections.sort(ports, new Comparator<CompactFieldNode>() {
      @Override
      public int compare(CompactFieldNode a, CompactFieldNode b) {
        return a.getName().compareTo(b.getName());
      }
    });
    return ports;
  }
  
  private void getAllPortsWithAncestor(TypeGraphVertex portVertex,
      TypeGraphVertex tgv, List<CompactFieldNode> ports)
  {
    List<CompactFieldNode> fields = tgv.getClassNode().getPorts();
    if (fields != null) {
      for (CompactFieldNode field : fields) {
        TypeGraphVertex fieldVertex = typeGraph.get(field.getDescription());

        if (isAncestor(portVertex, fieldVertex)) {
          ports.add(field);
        }
      }
    }
    for (TypeGraphVertex ancestor : tgv.ancestors) {
      getAllPortsWithAncestor(portVertex, ancestor, ports);
    }
  }

  private void addClassPropertiesAndPorts(String clazzName, JSONObject desc) throws JSONException {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    if (tgv == null) {
      return;
    }

    Map<String, JSONObject> results = new TreeMap<String, JSONObject>();
    List<CompactMethodNode> getters =  new LinkedList<CompactMethodNode>();
    List<CompactMethodNode> setters = new LinkedList<CompactMethodNode>();
    Map<Type, Type> typeReplacement = new HashMap<Type, Type>();
    List<CompactFieldNode> ports =  new LinkedList<CompactFieldNode>();
    
    getPublicSetterGetterAndPorts(tgv, setters, getters, typeReplacement, ports);
    desc.put("portTypeInfo", getPortTypeInfo(clazzName, typeReplacement, ports));
    
    for (CompactMethodNode setter : setters) {
      String prop = WordUtils.uncapitalize(setter.getName().substring(3));
      JSONObject propJ = results.get(prop);
      if (propJ == null) {
        propJ = new JSONObject();
        propJ.put("name", prop);
        results.put(prop, propJ);
      }
      propJ.put("canSet", true);
      propJ.put("canGet", false);


      MethodSignatureVisitor msv = null;
      msv = setter.getMethodSignatureNode();
      if(msv==null){
        continue;
      }
      
      List<Type> param = msv.getParameters();
      if (CollectionUtils.isEmpty(param)) {
        propJ.put("type", "UNKNOWN");
      } else {
        // only one param in setter method
        setTypes(propJ, param.get(0), typeReplacement);
        // propJ.put("type", param.getTypeObj().getClassName());

      }
      // propJ.put("type", typeString);
    }

    for (CompactMethodNode getter : getters) {
      int si = getter.getName().startsWith("is") ? 2 : 3;
      String prop = WordUtils.uncapitalize(getter.getName().substring(si));
      JSONObject propJ = results.get(prop);
      if (propJ == null) {
        propJ = new JSONObject();
        propJ.put("name", prop);
        results.put(prop, propJ);
        propJ.put("canSet", false);
        // propJ.put("type", Type.getReturnType(getter.desc).getClassName());

        MethodSignatureVisitor msv = null;
        msv = getter.getMethodSignatureNode();
        if(msv==null){
          continue;
        }
        

        Type rt = msv.getReturnType();
        if (rt == null) {
          propJ.put("type", "UNKNOWN");
        } else {
          setTypes(propJ, rt, typeReplacement);
          // propJ.put("type", param.getTypeObj().getClassName());
        }

      }

      propJ.put("canGet", true);
    }

    desc.put("properties", results.values());
  }

  private void getPublicSetterGetterAndPorts(TypeGraphVertex tgv, List<CompactMethodNode> setters, List<CompactMethodNode> getters, Map<Type, Type> typeReplacement, List<CompactFieldNode> ports)
  {
    CompactClassNode exClass = null;
    // check if the class needs to be excluded
    for (String e : EXCLUDE_CLASSES) {
      if(e.equals(tgv.getClassNode().getName())) {
        exClass = tgv.getClassNode();
        break;
      }
    }
    if (exClass != null) {
      // if it's visiting classes need to be exclude from parsing, remove methods that override in sub class
      // So the setter/getter methods in Operater, Object, Class won't be counted
      for (CompactMethodNode compactMethodNode : exClass.getGetterMethods()) {
        for (Iterator<CompactMethodNode> iterator = getters.iterator(); iterator.hasNext();) {
          CompactMethodNode cmn = iterator.next();
          if (cmn.getName().equals(compactMethodNode.getName())) {
            iterator.remove();
          }
        }
      }
      for (CompactMethodNode compactMethodNode : exClass.getSetterMethods()) {
        for (Iterator<CompactMethodNode> iterator = setters.iterator(); iterator.hasNext();) {
          CompactMethodNode cmn = iterator.next();
          if (cmn.getName().equals(compactMethodNode.getName())) {
            iterator.remove();
          }
        }
      }
    } else {
      if (tgv.getClassNode().getSetterMethods() != null) {
        setters.addAll(tgv.getClassNode().getSetterMethods());
      }
      if (tgv.getClassNode().getGetterMethods() != null) {
        getters.addAll(tgv.getClassNode().getGetterMethods());
      }
    }
    
    TypeGraphVertex portVertex = typeGraph.get(Operator.Port.class.getName());
    List<CompactFieldNode> fields = tgv.getClassNode().getPorts();
    if(fields != null) {
      for (CompactFieldNode field : fields) {
        TypeGraphVertex fieldVertex = typeGraph.get(field.getDescription());
        if (isAncestor(portVertex, fieldVertex)) {
          ports.add(field);
        }
      }
    }
    
    ClassSignatureVisitor csv = tgv.getClassNode().getCsv();
    Type superC = csv.getSuperClass();
    
    addReplacement(superC, typeReplacement);

    if(csv.getInterfaces()!=null){
      for(Type it : csv.getInterfaces()){
        addReplacement(it, typeReplacement);
      };
    }
    for (TypeGraphVertex ancestor : tgv.ancestors) {
      getPublicSetterGetterAndPorts(ancestor, setters, getters, typeReplacement, ports);
    }
  }

  private void addReplacement(Type superT, Map<Type, Type> typeReplacement)
  {
    if(superT!=null && superT instanceof ParameterizedTypeNode){
      Type[] actualTypes = ((ParameterizedTypeNode)superT).getActualTypeArguments();
      List<TypeVariableNode> tvs = typeGraph.get(((ParameterizedTypeNode)superT).getTypeObj().getClassName()).getClassNode().getCsv().getTypeV();
      int i = 0;
      for (TypeVariableNode typeVariableNode : tvs) {
        typeReplacement.put(typeVariableNode, actualTypes[i++]);
      }
    }
  }

  private void setTypes(JSONObject propJ, Type rawType, Map<Type, Type> typeReplacement) throws JSONException
  {
    // type could be replaced
    Type t = resolveType(rawType, typeReplacement);
    
    if (propJ == null) {
      return;
    } else {
      if (t instanceof WildcardTypeNode) {
        propJ.put("type", "?");
      } else if (t instanceof TypeNode) {
        TypeNode tn = (TypeNode) t;
        String typeS = tn.getTypeObj().getClassName();
        if (classReplacement.get(typeS) != null) {
          typeS = classReplacement.get(typeS);
        }
        propJ.put("type", typeS);
        UI_TYPE uiType = UI_TYPE.getEnumFor(typeS, typeGraph);
        if (uiType != null) {
          propJ.put("uiType", uiType.getName());
        }
        if (t instanceof ParameterizedTypeNode) {
          JSONArray jArray = new JSONArray();
          for (Type ttn : ((ParameterizedTypeNode) t).getActualTypeArguments()) {
            JSONObject objJ = new JSONObject();
            setTypes(objJ, ttn, typeReplacement);
            jArray.put(objJ);
          }
          propJ.put("typeArgs", jArray);
        }
      }
      if (t instanceof WildcardTypeNode) {
        JSONObject typeBounds = new JSONObject();
        
        
        JSONArray jArray = new JSONArray();
        Type[] bounds = ((WildcardTypeNode) t).getUpperBounds();
        if(bounds!=null){
          for (Type type : bounds) {
            jArray.put(type.toString());
          }
        }
        typeBounds.put("upper", jArray);
        
        bounds = ((WildcardTypeNode) t).getLowerBounds();

        jArray = new JSONArray();
        if(bounds!=null){
          for (Type type : bounds) {
            jArray.put(type.toString());
          }
        }
        typeBounds.put("lower", jArray);

        propJ.put("typeBounds", typeBounds);

      }
      if(t instanceof ArrayTypeNode){
        propJ.put("type", t.getByteString());
        propJ.put("uiType", UI_TYPE.LIST.getName());
        
        JSONObject jObj = new JSONObject();
        setTypes(jObj, ((ArrayTypeNode)t).getActualArrayType(), typeReplacement);
        propJ.put("itemType", jObj);
      }
      
      if(t instanceof TypeVariableNode){
        propJ.put("typeLiteral", ((TypeVariableNode)t).getTypeLiteral());
        setTypes(propJ, ((TypeVariableNode)t).getRawTypeBound(), typeReplacement);
      }


    }
  }

  
  
  private Type resolveType(Type t, Map<Type, Type> typeReplacement)
  {
    if(typeReplacement.containsKey(t)){
      return resolveType(typeReplacement.get(t), typeReplacement);
    } else {
      return t;
    }
  }

  /**
   * Type graph is big bidirectional object graph which can not be serialized by kryo.
   * This class is alternative {@link TypeGraph} kryo serializer
   * The serializer rule is
   * #ofNodes + node array + relationship array(int array which the value is index of the node array)
   */
  public static class TypeGraphSerializer extends Serializer<TypeGraph>
  {

    @Override
    public void write(Kryo kryo, Output output, TypeGraph tg)
    {
      Map<String, Integer> indexes = new HashMap<String, Integer>();
      // write the size first
      kryo.writeObject(output, tg.typeGraph.size());
      int i = 0;
      // Sequentially write the vertexes
      for (Entry<String, TypeGraphVertex> e : tg.typeGraph.entrySet()) {
        indexes.put(e.getKey(), i++);
        kryo.writeObject(output, e.getValue());
      }
      
      // Sequentially store the descendants and initializable descendants relationships in index in vertex array
      for (Entry<String, TypeGraphVertex> e : tg.typeGraph.entrySet()) {
        int[] refs = fromSet(e.getValue().descendants, indexes);
        kryo.writeObject(output, refs);
        refs = fromSet(e.getValue().allInitialiazableDescendants, indexes);
        kryo.writeObject(output, refs);
      }
      
    }

    private int[] fromSet(Set<TypeGraphVertex> tgvSet, Map<String, Integer> indexes)
    {
      int[] result = new int[tgvSet.size()];
      int j = 0;
      for (TypeGraphVertex t : tgvSet) {
        result[j++] = indexes.get(t.typeName);
      }
      return result;
    }

    @Override
    public TypeGraph read(Kryo kryo, Input input, Class<TypeGraph> type)
    {
      // read the #vertex
      int vertexNo = kryo.readObject(input, Integer.class);
      // read the vertexes into array
      TypeGraphVertex[] tgv = new TypeGraphVertex[vertexNo];
      for (int i = 0; i < vertexNo; i++) {
        tgv[i] = kryo.readObject(input, TypeGraphVertex.class);
      }
      
      // build relations between vertexes
      for (int i = 0; i < tgv.length; i++) {
        int[] ref = kryo.readObject(input, int[].class);
        for (int j = 0; j < ref.length; j++) {
          tgv[i].descendants.add(tgv[ref[j]]);
          tgv[ref[j]].ancestors.add(tgv[i]);
        }
        
        ref = kryo.readObject(input, int[].class);
        for (int j = 0; j < ref.length; j++) {
          tgv[i].allInitialiazableDescendants.add(tgv[ref[j]]);
        }
        
      }
      TypeGraph result = new TypeGraph();
      for (TypeGraphVertex typeGraphVertex : tgv) {
        result.typeGraph.put(typeGraphVertex.typeName, typeGraphVertex);
      }
      return result;
    }

  }

  /**
   * @param className
   * @return immediate parent names of given className
   * null if the className is not in this typeGraph or if there is no ancestors
   */
  public List<String> getParents(String className)
  {
    TypeGraphVertex tgv = typeGraph.get(className);
    if(tgv == null || tgv.ancestors == null){
      return null;
    }
    List<String> result = new LinkedList<String>();
    for (TypeGraphVertex p : tgv.ancestors) {
      result.add(p.typeName);
    }
    return result;
  }

}
