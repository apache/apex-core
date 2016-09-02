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
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.deser.std.FromStringDeserializer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.xbean.asm5.ClassReader;
import org.apache.xbean.asm5.tree.ClassNode;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;

import com.datatorrent.api.Component;
import com.datatorrent.api.DAG.GenericOperator;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.webapp.asm.ClassNodeType;
import com.datatorrent.stram.webapp.asm.ClassSignatureVisitor;
import com.datatorrent.stram.webapp.asm.CompactClassNode;
import com.datatorrent.stram.webapp.asm.CompactFieldNode;
import com.datatorrent.stram.webapp.asm.CompactMethodNode;
import com.datatorrent.stram.webapp.asm.CompactUtil;
import com.datatorrent.stram.webapp.asm.FastClassIndexReader;
import com.datatorrent.stram.webapp.asm.MethodSignatureVisitor;
import com.datatorrent.stram.webapp.asm.Type;
import com.datatorrent.stram.webapp.asm.Type.ArrayTypeNode;
import com.datatorrent.stram.webapp.asm.Type.ParameterizedTypeNode;
import com.datatorrent.stram.webapp.asm.Type.TypeNode;
import com.datatorrent.stram.webapp.asm.Type.TypeVariableNode;
import com.datatorrent.stram.webapp.asm.Type.WildcardTypeNode;

/**
 * A graph data structure holds all type information and their relationship needed in app builder
 * ASM is used to retrieve fields, methods information and kept in java beans and then stored in this graph data
 * structure
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
      Component.class.getName().replace('.', '/'),
      BaseOperator.class.getName().replace('.', '/')};

  public static final ImmutableSet<String> JACKSON_INSTANTIABLE_CLASSES;

  static {
    ImmutableSet.Builder<String> b = ImmutableSet.builder();

    for (Class pc : Primitives.allWrapperTypes()) {
      b.add(pc.getName());
    }
    Iterator<FromStringDeserializer<?>> iter = FromStringDeserializer.all().iterator();
    while (iter.hasNext()) {
      FromStringDeserializer fsd = iter.next();
      b.add(fsd.getValueClass().getName());
    }
    JACKSON_INSTANTIABLE_CLASSES = b.build();
  }

  public boolean isAncestor(String parentClassName, String subClassName)
  {
    TypeGraphVertex parentVertex = typeGraph.get(parentClassName);
    TypeGraphVertex classVertex = typeGraph.get(subClassName);

    if (parentVertex == null || classVertex == null) {
      return false;
    }

    return TypeGraph.isAncestor(parentVertex, classVertex);
  }

  public TypeGraphVertex getTypeGraphVertex(String className)
  {
    return typeGraph.get(className);
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
      if (isAncestor(typeTgv, vertex)) {
        return true;
      }
    }
    return false;
  }

  public TypeGraphVertex getNode(String typeName)
  {
    return typeGraph.get(typeName);
  }

  enum UI_TYPE
  {

    LIST("List", Collection.class.getName()),

    ENUM("Enum", Enum.class.getName()),

    MAP("Map", Map.class.getName()),

    /*
    Refer to https://fasterxml.github.io/jackson-databind/javadoc/2.4/com/fasterxml/jackson/databind/deser/std/FromStringDeserializer.html
     */

    STRING("java.lang.String", GetStringTypes()),

    INT("int", Integer.class.getName()),
    BYTE("byte", Byte.class.getName()),
    SHORT("short", Short.class.getName()),
    LONG("long", Long.class.getName()),
    DOUBLE("double", Double.class.getName()),
    FLOAT("float", Float.class.getName()),
    CHARACTER("char", Character.class.getName()),
    BOOLEAN("boolean", Boolean.class.getName());

    private static String[] GetStringTypes()
    {
      ArrayList<String> l = new ArrayList<>();
      l.add(Class.class.getName());
      Iterator<FromStringDeserializer<?>> iter = FromStringDeserializer.all().iterator();
      while (iter.hasNext()) {
        FromStringDeserializer fsd = iter.next();
        l.add(fsd.getValueClass().getName());
      }
      String[] a = new String[l.size()];
      return l.toArray(a);
    }

    private final String[] allAssignableTypes;
    private final String name;

    UI_TYPE(String name, String... allAssignableTypes)
    {
      this.allAssignableTypes = allAssignableTypes;
      this.name = name;
    }

    public static UI_TYPE getEnumFor(String clazzName, Map<String, TypeGraphVertex> typeGraph)
    {
      TypeGraphVertex tgv = typeGraph.get(clazzName);
      if (tgv == null) {
        return null;
      }
      for (UI_TYPE type : UI_TYPE.values()) {
        for (String assignable : type.allAssignableTypes) {
          TypeGraphVertex typeTgv = typeGraph.get(assignable);
          if (typeTgv == null) {
            continue;
          }
          if (isAncestor(typeTgv, tgv)) {
            return type;
          }
        }
      }
      return null;
    }

    public static UI_TYPE getEnumFor(TypeGraphVertex tgv)
    {
      List<String> allTypes = TypeGraph.getAllAncestors(tgv, true);
      for (UI_TYPE type : UI_TYPE.values()) {
        for (String assignable : type.allAssignableTypes) {
          if (allTypes.contains(assignable)) {
            return type;
          }
        }
      }
      return null;
    }

    public String getName()
    {
      return name;
    }
  }

  public static List<String> getAllAncestors(TypeGraphVertex tgv, boolean include)
  {
    List<String> result = new LinkedList<>();
    if (include) {
      result.add(tgv.typeName);
    }
    getAllAncestors(tgv, result);
    return result;
  }

  private static void getAllAncestors(TypeGraphVertex tgv, List<String> result)
  {
    for (TypeGraphVertex an : tgv.ancestors) {
      result.add(an.typeName);
      getAllAncestors(an, result);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TypeGraph.class);

  private final Map<String, TypeGraphVertex> typeGraph = new HashMap<>();

  private TypeGraphVertex addNode(InputStream input, String resName) throws IOException
  {
    try {

      FastClassIndexReader fastClassIndexReader = new FastClassIndexReader(input);
      String typeName = fastClassIndexReader.getName().replace('/', '.');
      TypeGraphVertex tgv;
      TypeGraphVertex ptgv;
      if (typeGraph.containsKey(typeName)) {
        tgv = typeGraph.get(typeName);
        tgv.setIsRealNode(true);
        tgv.setJarName(resName); // If tgv was already populated for superclass/interface, jar name needs to be updated
        tgv.setIsInstantiable(fastClassIndexReader.isInstantiable());
      } else {
        tgv = new TypeGraphVertex(this, typeName, resName, true, fastClassIndexReader.isInstantiable());
        typeGraph.put(typeName, tgv);
      }
      String immediateP = fastClassIndexReader.getSuperName();
      if (immediateP != null) {
        immediateP = immediateP.replace('/', '.');
        ptgv = typeGraph.get(immediateP);
        if (ptgv == null) {
          ptgv = new TypeGraphVertex(this, immediateP, resName);
          typeGraph.put(immediateP, ptgv);
        }
        tgv.ancestors.add(ptgv);
        ptgv.descendants.add(tgv);
      }
      if (fastClassIndexReader.getInterfaces() != null) {
        for (String iface : fastClassIndexReader.getInterfaces()) {
          iface = iface.replace('/', '.');
          ptgv = typeGraph.get(iface);
          if (ptgv == null) {
            ptgv = new TypeGraphVertex(this, iface, resName);
            typeGraph.put(iface, ptgv);
          }
          tgv.ancestors.add(ptgv);
          ptgv.descendants.add(tgv);
        }
      }

      updateInstantiableDescendants(tgv);
      return tgv;
    } finally {
      if (input != null) {
        input.close();
      }
    }
  }

  public TypeGraphVertex addNode(File file) throws IOException
  {
    return addNode(new FileInputStream(file), file.getAbsolutePath());
  }

  public TypeGraphVertex addNode(JarEntry jarEntry, JarFile jar) throws IOException
  {
    return addNode(jar.getInputStream(jarEntry), jar.getName());
  }

  private void updateInstantiableDescendants(TypeGraphVertex tgv)
  {
    if (tgv.isInstantiable()) {
      tgv.allInstantiableDescendants.add(tgv);
    }
    for (TypeGraphVertex parent : tgv.ancestors) {
      updateInstantiableDescendants(parent, tgv.allInstantiableDescendants);
    }
  }

  private void updateInstantiableDescendants(TypeGraphVertex tgv, Set<TypeGraphVertex> allChildren)
  {

    tgv.allInstantiableDescendants.addAll(allChildren);

    for (TypeGraphVertex parent : tgv.ancestors) {
      updateInstantiableDescendants(parent, allChildren);
    }
  }

  public int size()
  {
    return typeGraph.size();
  }

  public Set<String> getAllDTInstantiableGenericOperators()
  {
    TypeGraphVertex tgv = typeGraph.get(GenericOperator.class.getName());
    if (tgv == null) {
      return null;
    }
    Set<String> result = new TreeSet<>();
    for (TypeGraphVertex node : tgv.allInstantiableDescendants) {
      if ((isAncestor(InputOperator.class.getName(), node.typeName) || isAncestor(Module.class.getName(), node.typeName)
          || !getAllInputPorts(node).isEmpty())) {
        result.add(node.typeName);
      }
    }

    return result;
  }

  public Set<String> getDescendants(String fullClassName)
  {
    Set<String> result = new HashSet<>();
    TypeGraphVertex tgv = typeGraph.get(fullClassName);
    if (tgv != null) {
      tranverse(tgv, false, result, Integer.MAX_VALUE);
    }
    return result;
  }

  public List<String> getInstantiableDescendants(String fullClassName)
  {
    return getInstantiableDescendants(fullClassName, null, null, null);
  }

  private void tranverse(TypeGraphVertex tgv, boolean onlyInstantiable, Set<String> result, int limit)
  {
    if (!onlyInstantiable) {
      result.add(tgv.typeName);
    }

    if (onlyInstantiable && tgv.numberOfInstantiableDescendants() > limit) {
      throw new RuntimeException("Too many public concrete sub types!");
    }

    if (onlyInstantiable && tgv.isInstantiable()) {
      result.add(tgv.typeName);
    }

    if (tgv.descendants.size() > 0) {
      for (TypeGraphVertex child : tgv.descendants) {
        tranverse(child, onlyInstantiable, result, limit);
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
     * All instantiable(public type with a public non-arg constructor) implementations including direct and indirect descendants
     */
    private final transient SortedSet<TypeGraphVertex> allInstantiableDescendants = new TreeSet<>(new Comparator<TypeGraphVertex>()
    {
      @Override
      public int compare(TypeGraphVertex o1, TypeGraphVertex o2)
      {
        String n1 = o1.typeName;
        String n2 = o2.typeName;
        if (n1.startsWith("java")) {
          n1 = "0" + n1;
        }
        if (n2.startsWith("java")) {
          n2 = "0" + n2;
        }
        return n1.compareTo(n2);
      }
    });

    private final transient Set<TypeGraphVertex> ancestors = new HashSet<>();

    private final transient Set<TypeGraphVertex> descendants = new HashSet<>();

    private transient TypeGraph owner;

    // keep the jar file name for late fetching the detail information
    private String jarName;

    private boolean hasResource = false;

    private boolean isRealNode = false;

    private boolean isInstantiable = false;

    @SuppressWarnings("unused")
    private TypeGraphVertex()
    {
      jarName = "";
      typeName = "";
    }

    public TypeGraphVertex(TypeGraph owner, String typeName, String jarName, boolean isRealNode, boolean isInstantiable)
    {
      this.typeName = typeName;
      this.jarName = jarName;
      this.isRealNode = isRealNode;
      this.isInstantiable = isInstantiable;
      this.owner = owner;
    }

    public TypeGraphVertex(TypeGraph owner, String typeName, String jarName)
    {
      this(owner, typeName, jarName, false, false);
    }

    public void setOwner(TypeGraph owner)
    {
      this.owner = owner;
    }

    public TypeGraph getOwner()
    {
      return owner;
    }

    public Set<TypeGraphVertex> getAncestors()
    {
      return ancestors;
    }

    public int numberOfInstantiableDescendants()
    {
      return allInstantiableDescendants.size() + (isInstantiable() ? 1 : 0);
    }

    public boolean hasResource()
    {
      return hasResource;
    }

    public void setHasResource(boolean hasResource)
    {
      this.hasResource = hasResource;
    }

    public boolean isInstantiable()
    {
      return isInstantiable || JACKSON_INSTANTIABLE_CLASSES.contains(this.typeName);
    }

    public void setIsInstantiable(boolean isInstantiable)
    {
      this.isInstantiable = isInstantiable;
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
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TypeGraphVertex other = (TypeGraphVertex)obj;
      if (typeName == null) {
        if (other.typeName != null) {
          return false;
        }
      } else if (!typeName.equals(other.typeName)) {
        return false;
      }
      return true;
    }

    public String getJarName()
    {
      return jarName;
    }

    public void setJarName(String jarName)
    {
      this.jarName = jarName;
    }

    /**
     * The query on this vertex is possible to be called by multithread
     * Thus make this method synchronized
     * @return
     */
    public synchronized CompactClassNode getOrLoadClassNode()
    {
      if (classNode == null) {
        //load the class first
        try {
          loadClass();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return classNode;
    }

    public void setIsRealNode(boolean isRealNode)
    {
      this.isRealNode = isRealNode;
    }

    public boolean isRealNode()
    {
      return isRealNode;
    }

    private void loadClass() throws IOException
    {
      if (classNode != null) {
        return;
      }
      for (TypeGraphVertex ancestor : getAncestors()) {
        ancestor.loadClass();
      }
      JarFile jarFile = null;
      InputStream inputStream = null;
      try {
        if (jarName.endsWith(".jar")) {
          JarFile jarfile = new JarFile(jarName);
          inputStream = jarfile.getInputStream(jarfile.getEntry(typeName.replace('.', '/') + ".class"));
        } else {
          inputStream = new FileInputStream(jarName);
        }
        ClassReader reader = new ClassReader(inputStream);
        ClassNode classN = new ClassNodeType();
        reader.accept(classN, ClassReader.SKIP_CODE);
        CompactClassNode ccn = CompactUtil.compactClassNode(classN);
        this.classNode = ccn;

        // update the port information if it is a Operator
        if (owner.isAncestor(GenericOperator.class.getName(), typeName)) {
          // load ports if it is an Operator class
          CompactUtil.updateCompactClassPortInfo(classN, ccn);
          List<CompactFieldNode> prunedFields = new LinkedList<>();
          TypeGraphVertex portVertex = owner.getTypeGraphVertex(Operator.Port.class.getName());
          for (CompactFieldNode field : ccn.getPorts()) {
            TypeGraphVertex fieldVertex = owner.getTypeGraphVertex(field.getDescription());
            if (fieldVertex != null) {
              if (isAncestor(portVertex, fieldVertex)) {
                prunedFields.add(field);
              }
            }
          }
          ccn.setPorts(prunedFields);
        }
      } finally {
        IOUtils.closeQuietly(jarFile);
        IOUtils.closeQuietly(inputStream);
      }

    }

  }

  /**
   * @param clazz parent class
   * @param filter
   * @param packagePrefix
   * @param startsWith  case insensitive
   * @return all instantiable descendants of class clazz which comfort to filter expression, packagePrefix and start
   * with $startsWith
   */
  public List<String> getInstantiableDescendants(String clazz, String filter, String packagePrefix, String startsWith)
  {
    TypeGraphVertex tgv = typeGraph.get(clazz);
    if (tgv == null) {
      return null;
    }

    List<String> result = new LinkedList<>();
    if (tgv != null) {
      for (TypeGraphVertex node : tgv.allInstantiableDescendants) {
        String typeName = node.typeName;
        if (filter != null && !Pattern.matches(filter, node.typeName)) {
          continue;
        }
        if (packagePrefix != null && !node.typeName.startsWith(packagePrefix)) {
          continue;
        }
        if (startsWith != null && !typeName.substring(typeName.lastIndexOf('.') + 1).toLowerCase()
            .startsWith(startsWith.toLowerCase())) {
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
    CompactClassNode cn = tgv.getOrLoadClassNode();
    if (cn.isEnum()) {

      List<String> enumNames = cn.getEnumValues();
      desc.put("enum", enumNames);
      desc.put("uiType", UI_TYPE.ENUM.getName());
    }
    UI_TYPE uType = UI_TYPE.getEnumFor(tgv.typeName, typeGraph);
    if (uType != null) {
      desc.put("uiType", uType.getName());
    }
    addClassPropertiesAndPorts(clazzName, desc);
    if (tgv.hasResource()) {
      desc.put("hasResource", "true");
    } else {
      desc.put("hasResource", "false");
    }

    return desc;
  }

  private Collection<JSONObject> getPortTypeInfo(String clazzName,
      Map<Type, Type> typeReplacement, List<CompactFieldNode> ports) throws JSONException
  {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    if (tgv == null) {
      return null;
    }

    Collection<JSONObject> portInfo = new ArrayList<>();

    for (CompactFieldNode port : ports) {
      Type fieldType = port.getFieldSignatureNode().getFieldType();
      Type t = fieldType;
      if (fieldType instanceof ParameterizedTypeNode) {
        // TODO: Right now getPortInfo assumes a single parameterized type
        t = ((ParameterizedTypeNode)fieldType).getActualTypeArguments()[0];
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
        throw Throwables.propagate(e);
      }
    }

    return portInfo;
  }

  public static Type getParameterizedTypeArgument(Type type)
  {
    if (type instanceof ParameterizedTypeNode) {
      return ((ParameterizedTypeNode)type).getActualTypeArguments()[0];
    }
    return null;
  }

  private Type findTypeArgument(TypeGraphVertex tgv, Map<Type, Type> typeReplacement)
  {
    if (tgv == null) {
      return null;
    }
    ClassSignatureVisitor csv = tgv.getOrLoadClassNode().getCsv();
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

  public List<CompactFieldNode> getAllInputPorts(String clazzName)
  {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    return getAllInputPorts(tgv);
  }

  public List<CompactFieldNode> getAllInputPorts(TypeGraphVertex tgv)
  {
    List<CompactFieldNode> ports = new ArrayList<>();
    if (tgv == null) {
      return ports;
    }
    TypeGraphVertex portVertex = typeGraph.get(Operator.InputPort.class.getName());
    getAllPortsWithAncestor(portVertex, tgv, ports);
    Collections.sort(ports, new Comparator<CompactFieldNode>()
    {
      @Override
      public int compare(CompactFieldNode a, CompactFieldNode b)
      {
        return a.getName().compareTo(b.getName());
      }
    });

    return ports;
  }

  public List<CompactFieldNode> getAllOutputPorts(String clazzName)
  {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    List<CompactFieldNode> ports = new ArrayList<>();
    TypeGraphVertex portVertex = typeGraph.get(Operator.OutputPort.class.getName());
    getAllPortsWithAncestor(portVertex, tgv, ports);
    Collections.sort(ports, new Comparator<CompactFieldNode>()
    {
      @Override
      public int compare(CompactFieldNode a, CompactFieldNode b)
      {
        return a.getName().compareTo(b.getName());
      }
    });
    return ports;
  }

  private void getAllPortsWithAncestor(TypeGraphVertex portVertex, TypeGraphVertex tgv, List<CompactFieldNode> ports)
  {
    List<CompactFieldNode> fields = tgv.getOrLoadClassNode().getPorts();
    if (fields != null) {
      for (CompactFieldNode field : fields) {
        TypeGraphVertex fieldVertex = typeGraph.get(field.getDescription());

        if (isAncestor(portVertex, fieldVertex) && !isNodeInList(ports, field)) {
          ports.add(field);
        }
      }
    }
    for (TypeGraphVertex ancestor : tgv.ancestors) {
      getAllPortsWithAncestor(portVertex, ancestor, ports);
    }
  }

  private static boolean isNodeInList(List<CompactFieldNode> list, CompactFieldNode vertex)
  {
    for (CompactFieldNode node: list) {
      if (node.getName().equals(vertex.getName())) {
        return true;
      }
    }
    return false;
  }

  private void addClassPropertiesAndPorts(String clazzName, JSONObject desc) throws JSONException
  {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    if (tgv == null) {
      return;
    }

    Map<String, JSONObject> results = new TreeMap<>();
    List<CompactMethodNode> getters = new LinkedList<>();
    List<CompactMethodNode> setters = new LinkedList<>();
    Map<Type, Type> typeReplacement = new HashMap<>();
    List<CompactFieldNode> ports = new LinkedList<>();

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
      if (msv == null) {
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
        if (msv == null) {
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
      if (e.equals(tgv.getOrLoadClassNode().getName())) {
        exClass = tgv.getOrLoadClassNode();
        break;
      }
    }
    if (exClass != null) {
      // if it's visiting classes need to be exclude from parsing, remove methods that override in sub class
      // So the setter/getter methods in Operater, Object, Class won't be counted
      for (CompactMethodNode compactMethodNode : exClass.getGetterMethods()) {
        for (Iterator<CompactMethodNode> iterator = getters.iterator(); iterator.hasNext(); ) {
          CompactMethodNode cmn = iterator.next();
          if (cmn.getName().equals(compactMethodNode.getName())) {
            iterator.remove();
          }
        }
      }
      for (CompactMethodNode compactMethodNode : exClass.getSetterMethods()) {
        for (Iterator<CompactMethodNode> iterator = setters.iterator(); iterator.hasNext(); ) {
          CompactMethodNode cmn = iterator.next();
          if (cmn.getName().equals(compactMethodNode.getName())) {
            iterator.remove();
          }
        }
      }
    } else {
      if (tgv.getOrLoadClassNode().getSetterMethods() != null) {
        setters.addAll(tgv.getOrLoadClassNode().getSetterMethods());
      }
      if (tgv.getOrLoadClassNode().getGetterMethods() != null) {
        getters.addAll(tgv.getOrLoadClassNode().getGetterMethods());
      }
    }

    TypeGraphVertex portVertex = typeGraph.get(Operator.Port.class.getName());
    List<CompactFieldNode> fields = tgv.getOrLoadClassNode().getPorts();
    if (fields != null) {
      for (CompactFieldNode field : fields) {
        TypeGraphVertex fieldVertex = typeGraph.get(field.getDescription());
        if (isAncestor(portVertex, fieldVertex) && !isNodeInList(ports, field)) {
          ports.add(field);
        }
      }
    }

    ClassSignatureVisitor csv = tgv.getOrLoadClassNode().getCsv();
    Type superC = csv.getSuperClass();

    addReplacement(superC, typeReplacement);

    if (csv.getInterfaces() != null) {
      for (Type it : csv.getInterfaces()) {
        addReplacement(it, typeReplacement);
      }
    }
    for (TypeGraphVertex ancestor : tgv.ancestors) {
      getPublicSetterGetterAndPorts(ancestor, setters, getters, typeReplacement, ports);
    }
  }

  private void addReplacement(Type superT, Map<Type, Type> typeReplacement)
  {
    if (superT != null && superT instanceof ParameterizedTypeNode) {
      Type[] actualTypes = ((ParameterizedTypeNode)superT).getActualTypeArguments();
      List<TypeVariableNode> tvs = typeGraph.get(((ParameterizedTypeNode)superT).getTypeObj().getClassName()).getOrLoadClassNode().getCsv().getTypeV();
      int i = 0;
      for (TypeVariableNode typeVariableNode : tvs) {
        typeReplacement.put(typeVariableNode, actualTypes[i++]);
      }
    }
  }

  private void setTypes(JSONObject propJ, Type rawType, Map<Type, Type> typeReplacement) throws JSONException
  {
    setTypes(propJ, rawType, typeReplacement, new HashSet<Type>());
  }

  private void setTypes(JSONObject propJ, Type rawType, Map<Type, Type> typeReplacement, Set<Type> visitedType) throws JSONException
  {
    boolean stopRecursive = visitedType.contains(rawType);
    visitedType.add(rawType);
    // type could be replaced
    Type t = resolveType(rawType, typeReplacement);

    if (propJ == null) {
      return;
    } else {
      if (t instanceof WildcardTypeNode) {
        propJ.put("type", "?");
      } else if (t instanceof TypeNode) {
        TypeNode tn = (TypeNode)t;
        String typeS = tn.getTypeObj().getClassName();
        propJ.put("type", typeS);
        UI_TYPE uiType = UI_TYPE.getEnumFor(typeS, typeGraph);
        if (uiType != null) {
          switch (uiType) {
            case FLOAT:
            case LONG:
            case INT:
            case DOUBLE:
            case BYTE:
            case SHORT:
            case STRING:
              propJ.put("type", uiType.getName());
              break;
            default:
              propJ.put("uiType", uiType.getName());

          }

        }
        if (t instanceof ParameterizedTypeNode) {
          JSONArray jArray = new JSONArray();
          for (Type ttn : ((ParameterizedTypeNode)t).getActualTypeArguments()) {
            JSONObject objJ = new JSONObject();
            if (!stopRecursive) {
              setTypes(objJ, ttn, typeReplacement, visitedType);
            }
            jArray.put(objJ);
          }
          propJ.put("typeArgs", jArray);
        }
      }
      if (t instanceof WildcardTypeNode) {
        JSONObject typeBounds = new JSONObject();
        JSONArray jArray = new JSONArray();
        Type[] bounds = ((WildcardTypeNode)t).getUpperBounds();
        if (bounds != null) {
          for (Type type : bounds) {
            jArray.put(type.toString());
          }
        }
        typeBounds.put("upper", jArray);

        bounds = ((WildcardTypeNode)t).getLowerBounds();

        jArray = new JSONArray();
        if (bounds != null) {
          for (Type type : bounds) {
            jArray.put(type.toString());
          }
        }
        typeBounds.put("lower", jArray);

        propJ.put("typeBounds", typeBounds);

      }
      if (t instanceof ArrayTypeNode) {
        propJ.put("type", t.getByteString());
        propJ.put("uiType", UI_TYPE.LIST.getName());

        JSONObject jObj = new JSONObject();
        if (!stopRecursive) {
          setTypes(jObj, ((ArrayTypeNode)t).getActualArrayType(), typeReplacement, visitedType);
        }
        propJ.put("itemType", jObj);
      }

      if (t instanceof TypeVariableNode) {
        propJ.put("typeLiteral", ((TypeVariableNode)t).getTypeLiteral());
        if (!stopRecursive) {
          setTypes(propJ, ((TypeVariableNode)t).getRawTypeBound(), typeReplacement, visitedType);
        }
      }

    }
  }

  /*
   * Trim the node(s) that depends on external dependencies.
   * The classes that (in)directly implements/extends external classes can't be initialized anyways
   */
  public void trim()
  {
    List<TypeGraphVertex> invalidVertexes = new LinkedList<>();
    for (TypeGraphVertex tgv : typeGraph.values()) {
      if (!tgv.isRealNode()) {
        invalidVertexes.add(tgv);
      }
    }

    for (TypeGraphVertex removeV : invalidVertexes) {
      // Removing node and all it's (in)direct descendants
      removeSubGraph(removeV);
    }
  }

  private void removeSubGraph(TypeGraphVertex v)
  {

    // Can't recursively remove because it will get into concurrent modification
    // Use queue to delete all nodes
    Queue<TypeGraphVertex> removingQueue = new LinkedList<>();
    removingQueue.add(v);
    while (!removingQueue.isEmpty()) {
      TypeGraphVertex tgv = removingQueue.poll();
      if (typeGraph.get(tgv.typeName) == null) {
        // skip node that's been removed already.
        // It comes from common descendants
        continue;
      }
      // put all the descendants to waiting queue
      for (TypeGraphVertex child : tgv.descendants) {
        removingQueue.offer(child);
      }
      // remove from global hashmap
      typeGraph.remove(tgv.typeName);
      // remove from instantiable descendants list of all the (in)direct ancestors
      if (!tgv.allInstantiableDescendants.isEmpty() && !tgv.ancestors.isEmpty()) {
        for (TypeGraphVertex p : tgv.ancestors) {
          removeFromInstantiableDescendants(p, tgv.allInstantiableDescendants);
        }
      }
      // cut links from parent to child
      for (TypeGraphVertex parent : tgv.ancestors) {
        parent.descendants.remove(tgv);
      }
      // cut links form child to parent
      tgv.ancestors.clear();
    }
  }

  private void removeFromInstantiableDescendants(TypeGraphVertex parentT, Set<TypeGraphVertex> childT)
  {
    for (TypeGraphVertex vertex : childT) {
      parentT.allInstantiableDescendants.remove(vertex);
    }
    for (TypeGraphVertex pt : parentT.ancestors) {
      removeFromInstantiableDescendants(pt, childT);
    }
  }

  private Type resolveType(Type t, Map<Type, Type> typeReplacement)
  {
    if (typeReplacement.containsKey(t)) {
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
      Map<String, Integer> indexes = new HashMap<>();
      // write the size first
      kryo.writeObject(output, tg.typeGraph.size());
      int i = 0;
      // Sequentially write the vertexes
      for (Entry<String, TypeGraphVertex> e : tg.typeGraph.entrySet()) {
        indexes.put(e.getKey(), i++);
        kryo.writeObject(output, e.getValue());
      }

      // Sequentially store the descendants and instantiable descendants relationships in index in vertex array
      for (Entry<String, TypeGraphVertex> e : tg.typeGraph.entrySet()) {
        int[] refs = fromSet(e.getValue().descendants, indexes);
        kryo.writeObject(output, refs);
        refs = fromSet(e.getValue().allInstantiableDescendants, indexes);
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
          tgv[i].allInstantiableDescendants.add(tgv[ref[j]]);
        }

      }
      TypeGraph result = new TypeGraph();
      for (TypeGraphVertex typeGraphVertex : tgv) {
        result.typeGraph.put(typeGraphVertex.typeName, typeGraphVertex);
        typeGraphVertex.setOwner(result);
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
    if (tgv == null || tgv.ancestors == null) {
      return null;
    }
    List<String> result = new LinkedList<>();
    for (TypeGraphVertex p : tgv.ancestors) {
      result.add(p.typeName);
    }
    return result;
  }

  /**
   * A utility method that tells whether a class is considered a bean.<br/>
   * For simplicity we exclude classes that have any type-args.
   *
   * @param className name of the class
   * @return true if it is a bean false otherwise.
   */
  public boolean isInstantiableBean(String className) throws JSONException
  {
    JSONObject classDesc = describeClass(className);
    if (classDesc.has("typeArgs")) {
      //any type with generics is not considered a bean
      return false;
    }
    JSONArray classProps = classDesc.optJSONArray("properties");
    if (classProps == null || classProps.length() == 0) {
      //no properties then cannot be a bean
      return false;
    }
    for (int p = 0; p < classProps.length(); p++) {
      JSONObject propDesc = classProps.getJSONObject(p);
      if (propDesc.optBoolean("canGet", false)) {
        return true;
      }
    }
    return false;
  }

}
