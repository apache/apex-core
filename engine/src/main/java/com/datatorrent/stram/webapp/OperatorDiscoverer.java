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

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.DAG.GenericOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.util.ObjectMapperFactory;
import com.datatorrent.stram.webapp.TypeDiscoverer.UI_TYPE;
import com.datatorrent.stram.webapp.TypeGraph.TypeGraphVertex;
import com.datatorrent.stram.webapp.asm.CompactAnnotationNode;
import com.datatorrent.stram.webapp.asm.CompactFieldNode;

/**
 * <p>OperatorDiscoverer class.</p>
 * Discover Operators.
 * Warning: Using this class may cause classloader leakage, depending on the classes it loads.
 *
 * @since 0.3.2
 */
public class OperatorDiscoverer
{
  public static final String GENERATED_CLASSES_JAR = "_generated-classes.jar";
  private Set<String> operatorClassNames;
  private static final Logger LOG = LoggerFactory.getLogger(OperatorDiscoverer.class);
  private final List<String> pathsToScan = new ArrayList<>();
  private final ClassLoader classLoader;
  private static final String DT_OPERATOR_DOCLINK_PREFIX = "https://www.datatorrent.com/docs/apidocs/index.html";
  public static final String PORT_TYPE_INFO_KEY = "portTypeInfo";
  private final TypeGraph typeGraph = TypeGraphFactory.createTypeGraphProtoType();

  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+?");

  private static final String SCHEMA_REQUIRED_KEY = "schemaRequired";

  private final Map<String, OperatorClassInfo> classInfo = new HashMap<>();

  private static class OperatorClassInfo
  {
    String comment;
    final Map<String, String> tags = new HashMap<>();
    final Map<String, MethodInfo> getMethods = new HashMap<>();
    final Map<String, MethodInfo> setMethods = new HashMap<>();
    final Map<String, String> fields = new HashMap<>();
  }

  private static class MethodInfo
  {
    Map<String, String> descriptions = new HashMap<>();
    Map<String, String> useSchemas = new HashMap<>();
    String comment;
    boolean omitFromUI;
  }

  enum GenericOperatorType
  {
    OPERATOR("operator"), MODULE("module");
    private final String type;

    private GenericOperatorType(String type)
    {
      this.type = type;
    }

    public String getType()
    {
      return type;
    }
  }

  enum MethodTagType
  {
    USE_SCHEMA("@useSchema"),
    DESCRIPTION("@description"),
    OMIT_FROM_UI("@omitFromUI");

    private static final Map<String, MethodTagType> TAG_TEXT_MAPPING = Maps.newHashMap();

    static {
      for (MethodTagType type : MethodTagType.values()) {
        TAG_TEXT_MAPPING.put(type.tag, type);
      }
    }

    private final String tag;

    MethodTagType(String tag)
    {
      this.tag = tag;
    }

    static MethodTagType from(String tag)
    {
      return TAG_TEXT_MAPPING.get(tag);
    }
  }

  private class JavadocSAXHandler extends DefaultHandler
  {
    private String className = null;
    private OperatorClassInfo oci = null;
    private StringBuilder comment;
    private String fieldName = null;
    private String methodName = null;
    private final Pattern getterPattern = Pattern.compile("(?:is|get)[A-Z].*");
    private final Pattern setterPattern = Pattern.compile("(?:set)[A-Z].*");

    private boolean isGetter(String methodName)
    {
      return getterPattern.matcher(methodName).matches();
    }

    private boolean isSetter(String methodName)
    {
      return setterPattern.matcher(methodName).matches();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException
    {
      if (qName.equalsIgnoreCase("class")) {
        className = attributes.getValue("qualified");
        oci = new OperatorClassInfo();
      } else if (qName.equalsIgnoreCase("comment")) {
        comment = new StringBuilder();
      } else if (qName.equalsIgnoreCase("tag")) {
        if (oci != null) {
          String tagName = attributes.getValue("name");
          String tagText = attributes.getValue("text").trim();
          if (methodName != null) {
            boolean lGetterCheck = isGetter(methodName);
            boolean lSetterCheck = !lGetterCheck && isSetter(methodName);

            if (lGetterCheck || lSetterCheck) {
              MethodTagType type = MethodTagType.from(tagName);
              if (type != null) {
                addTagToMethod(lGetterCheck ? oci.getMethods : oci.setMethods, tagText, type);
              }
            }
//            if ("@return".equals(tagName) && isGetter(methodName)) {
//              oci.getMethods.put(methodName, tagText);
//            }
            //do nothing
          } else if (fieldName != null) {
            // do nothing
          } else {
            oci.tags.put(tagName, tagText);
          }
        }
      } else if (qName.equalsIgnoreCase("field")) {
        fieldName = attributes.getValue("name");
      } else if (qName.equalsIgnoreCase("method")) {
        methodName = attributes.getValue("name");
      }
    }

    private void addTagToMethod(Map<String, MethodInfo> methods, String tagText, MethodTagType tagType)
    {
      MethodInfo mi = methods.get(methodName);
      if (mi == null) {
        mi = new MethodInfo();
        methods.put(methodName, mi);
      }
      if (tagType == MethodTagType.OMIT_FROM_UI) {
        mi.omitFromUI = true;
        return;
      }
      String[] tagParts = Iterables.toArray(Splitter.on(WHITESPACE_PATTERN).trimResults().omitEmptyStrings()
          .limit(2).split(tagText), String.class);
      if (tagParts.length == 2) {
        if (tagType == MethodTagType.DESCRIPTION) {
          mi.descriptions.put(tagParts[0], tagParts[1]);
        } else {
          mi.useSchemas.put(tagParts[0], tagParts[1]);
        }
      }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException
    {
      if (qName.equalsIgnoreCase("class")) {
        classInfo.put(className, oci);
        className = null;
        oci = null;
      } else if (qName.equalsIgnoreCase("comment") && oci != null) {
        if (methodName != null) {
          // do nothing
          if (isGetter(methodName)) {
            MethodInfo mi = oci.getMethods.get(methodName);
            if (mi == null) {
              mi = new MethodInfo();
              oci.getMethods.put(methodName, mi);
            }
            mi.comment = comment.toString();
          } else if (isSetter(methodName)) {
            MethodInfo mi = oci.setMethods.get(methodName);
            if (mi == null) {
              mi = new MethodInfo();
              oci.setMethods.put(methodName, mi);
            }
            mi.comment = comment.toString();
          }
        } else if (fieldName != null) {
          oci.fields.put(fieldName, comment.toString());
        } else {
          oci.comment = comment.toString();
        }
        comment = null;
      } else if (qName.equalsIgnoreCase("field")) {
        fieldName = null;
      } else if (qName.equalsIgnoreCase("method")) {
        methodName = null;
      }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException
    {
      if (comment != null) {
        comment.append(ch, start, length);
      }
    }
  }

  public OperatorDiscoverer()
  {
    classLoader = ClassLoader.getSystemClassLoader();
  }

  public OperatorDiscoverer(String[] jars)
  {
    URL[] urls = new URL[jars.length];
    for (int i = 0; i < jars.length; i++) {
      pathsToScan.add(jars[i]);
      try {
        urls[i] = new URL("file://" + jars[i]);
      } catch (MalformedURLException ex) {
        throw new RuntimeException(ex);
      }
    }
    classLoader = new URLClassLoader(urls, ClassLoader.getSystemClassLoader());
  }

  private void loadGenericOperatorClasses()
  {
    buildTypeGraph();
    operatorClassNames =  typeGraph.getAllDTInstantiableGenericOperators();
  }

  @SuppressWarnings("unchecked")
  public void addDefaultValue(String className, JSONObject oper) throws Exception
  {
    ObjectMapper defaultValueMapper = ObjectMapperFactory.getOperatorValueSerializer();
    Class<? extends GenericOperator> clazz = (Class<? extends GenericOperator>)classLoader.loadClass(className);
    if (clazz != null) {
      GenericOperator operIns = clazz.newInstance();
      String s = defaultValueMapper.writeValueAsString(operIns);
      oper.put("defaultValue", new JSONObject(s).get(className));
    }
  }

  public void buildTypeGraph()
  {
    Map<String, JarFile> openJarFiles = new HashMap<>();
    Map<String, File> openClassFiles = new HashMap<>();
    // use global cache to load resource in/out of the same jar as the classes
    Set<String> resourceCacheSet = new HashSet<>();
    try {
      for (String path : pathsToScan) {
        File f = null;
        try {
          f = new File(path);
          if (!f.exists() || f.isDirectory() || (!f.getName().endsWith("jar") && !f.getName().endsWith("class"))) {
            continue;
          }
          if (GENERATED_CLASSES_JAR.equals(f.getName())) {
            continue;
          }
          if (f.getName().endsWith("class")) {
            typeGraph.addNode(f);
            openClassFiles.put(path, f);
          } else {
            JarFile jar = new JarFile(path);
            openJarFiles.put(path, jar);
            java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
            while (entriesEnum.hasMoreElements()) {
              final java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
              String entryName = jarEntry.getName();
              if (jarEntry.isDirectory()) {
                continue;
              }
              if (entryName.endsWith("-javadoc.xml")) {
                try {
                  processJavadocXml(jar.getInputStream(jarEntry));
                  // break;
                } catch (Exception ex) {
                  LOG.warn("Cannot process javadoc {} : ", entryName, ex);
                }
              } else if (entryName.endsWith(".class")) {
                TypeGraph.TypeGraphVertex newNode = typeGraph.addNode(jarEntry, jar);
                // check if any visited resources belong to this type
                for (Iterator<String> iter = resourceCacheSet.iterator(); iter.hasNext(); ) {
                  String entry = iter.next();
                  if (entry.startsWith(entryName.substring(0, entryName.length() - 6))) {
                    newNode.setHasResource(true);
                    iter.remove();
                  }
                }
              } else {
                String className = entryName;
                boolean foundClass = false;
                // check if this resource belongs to any visited type
                while (className.contains("/")) {
                  className = className.substring(0, className.lastIndexOf('/'));
                  TypeGraph.TypeGraphVertex tgv = typeGraph.getNode(className.replace('/', '.'));
                  if (tgv != null) {
                    tgv.setHasResource(true);
                    foundClass = true;
                    break;
                  }
                }
                if (!foundClass) {
                  resourceCacheSet.add(entryName);
                }
              }
            }
          }
        } catch (IOException ex) {
          LOG.warn("Cannot process file {}", f, ex);
        }
      }

      typeGraph.trim();

    } finally {
      for (Entry<String, JarFile> entry : openJarFiles.entrySet()) {
        try {
          entry.getValue().close();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    }
  }

  private void processJavadocXml(InputStream is) throws ParserConfigurationException, SAXException, IOException
  {
    SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
    saxParserFactory.newSAXParser().parse(is, new JavadocSAXHandler());
  }

  public Set<String> getOperatorClasses(String parent, String searchTerm) throws ClassNotFoundException
  {
    if (CollectionUtils.isEmpty(operatorClassNames)) {
      loadGenericOperatorClasses();
    }
    if (parent == null) {
      parent = GenericOperator.class.getName();
    } else {
      if (!typeGraph.isAncestor(GenericOperator.class.getName(), parent)) {
        throw new IllegalArgumentException("Argument must be a subclass of Operator class");
      }
    }

    Set<String> filteredClass = Sets.filter(operatorClassNames, new Predicate<String>()
    {
      @Override
      public boolean apply(String className)
      {
        OperatorClassInfo oci = classInfo.get(className);
        return oci == null || !oci.tags.containsKey("@omitFromUI");
      }
    });

    if (searchTerm == null && parent.equals(GenericOperator.class.getName())) {
      return filteredClass;
    }

    if (searchTerm != null) {
      searchTerm = searchTerm.toLowerCase();
    }

    Set<String> result = new HashSet<>();
    for (String clazz : filteredClass) {
      if (parent.equals(GenericOperator.class.getName()) || typeGraph.isAncestor(parent, clazz)) {
        if (searchTerm == null) {
          result.add(clazz);
        } else {
          if (clazz.toLowerCase().contains(searchTerm)) {
            result.add(clazz);
          } else {
            OperatorClassInfo oci = classInfo.get(clazz);
            if (oci != null) {
              if (oci.comment != null && oci.comment.toLowerCase().contains(searchTerm)) {
                result.add(clazz);
              } else {
                for (Map.Entry<String, String> entry : oci.tags.entrySet()) {
                  if (entry.getValue().toLowerCase().contains(searchTerm)) {
                    result.add(clazz);
                    break;
                  }
                }
              }
            }
          }
        }
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Operator> getOperatorClass(String className) throws ClassNotFoundException
  {
    if (CollectionUtils.isEmpty(operatorClassNames)) {
      loadGenericOperatorClasses();
    }

    Class<?> clazz = classLoader.loadClass(className);

    if (!Operator.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Argument must be a subclass of Operator class");
    }
    return (Class<? extends Operator>)clazz;
  }

  public JSONObject describeOperator(String clazz) throws Exception
  {
    TypeGraphVertex tgv = typeGraph.getTypeGraphVertex(clazz);
    if (tgv.isInstantiable()) {
      JSONObject response = new JSONObject();
      JSONArray inputPorts = new JSONArray();
      JSONArray outputPorts = new JSONArray();
      // Get properties from ASM

      JSONObject operatorDescriptor =  describeClassByASM(clazz);
      JSONArray properties = operatorDescriptor.getJSONArray("properties");

      properties = enrichProperties(clazz, properties);

      JSONArray portTypeInfo = operatorDescriptor.getJSONArray("portTypeInfo");

      List<CompactFieldNode> inputPortfields = typeGraph.getAllInputPorts(clazz);
      List<CompactFieldNode> outputPortfields = typeGraph.getAllOutputPorts(clazz);


      try {
        for (CompactFieldNode field : inputPortfields) {
          JSONObject inputPort = setFieldAttributes(clazz, field);
          if (!inputPort.has("optional")) {
            inputPort.put("optional", false); // input port that is not annotated is default to be not optional
          }
          if (!inputPort.has(SCHEMA_REQUIRED_KEY)) {
            inputPort.put(SCHEMA_REQUIRED_KEY, false);
          }
          inputPorts.put(inputPort);
        }

        for (CompactFieldNode field : outputPortfields) {
          JSONObject outputPort = setFieldAttributes(clazz, field);

          if (!outputPort.has("optional")) {
            outputPort.put("optional", true); // output port that is not annotated is default to be optional
          }
          if (!outputPort.has("error")) {
            outputPort.put("error", false);
          }
          if (!outputPort.has(SCHEMA_REQUIRED_KEY)) {
            outputPort.put(SCHEMA_REQUIRED_KEY, false);
          }
          outputPorts.put(outputPort);
        }

        response.put("name", clazz);
        response.put("properties", properties);
        response.put(PORT_TYPE_INFO_KEY, portTypeInfo);
        response.put("inputPorts", inputPorts);
        response.put("outputPorts", outputPorts);
        String type = null;
        Class<?> genericOperator = classLoader.loadClass(clazz);
        if (Module.class.isAssignableFrom(genericOperator)) {
          type = GenericOperatorType.MODULE.getType();
        } else if (Operator.class.isAssignableFrom(genericOperator)) {
          type = GenericOperatorType.OPERATOR.getType();
        }
        if (type != null) {
          response.put("type", type);
        }

        OperatorClassInfo oci = classInfo.get(clazz);

        if (oci != null) {
          if (oci.comment != null) {
            String[] descriptions;
            // first look for a <p> tag
            String keptPrefix = "<p>";
            descriptions = oci.comment.split("<p>", 2);
            if (descriptions.length == 0) {
              keptPrefix = "";
              // if no <p> tag, then look for a blank line
              descriptions = oci.comment.split("\n\n", 2);
            }
            if (descriptions.length > 0) {
              response.put("shortDesc", descriptions[0]);
            }
            if (descriptions.length > 1) {
              response.put("longDesc", keptPrefix + descriptions[1]);
            }
          }
          response.put("category", oci.tags.get("@category"));
          String displayName = oci.tags.get("@displayName");
          if (displayName == null) {
            displayName = decamelizeClassName(ClassUtils.getShortClassName(clazz));
          }
          response.put("displayName", displayName);
          String tags = oci.tags.get("@tags");
          if (tags != null) {
            JSONArray tagArray = new JSONArray();
            for (String tag : StringUtils.split(tags, ',')) {
              tagArray.put(tag.trim().toLowerCase());
            }
            response.put("tags", tagArray);
          }
          String doclink = oci.tags.get("@doclink");
          if (doclink != null) {
            response.put("doclink", doclink + "?" + getDocName(clazz));
          } else if (clazz.startsWith("com.datatorrent.lib.") ||
              clazz.startsWith("com.datatorrent.contrib.")) {
            response.put("doclink", DT_OPERATOR_DOCLINK_PREFIX + "?" + getDocName(clazz));
          }
        }
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
      return response;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private JSONObject setFieldAttributes(String clazz, CompactFieldNode field) throws JSONException
  {
    JSONObject port = new JSONObject();
    port.put("name", field.getName());

    TypeGraphVertex tgv = typeGraph.getTypeGraphVertex(clazz);
    putFieldDescription(field, port, tgv);

    List<CompactAnnotationNode> annotations = field.getVisibleAnnotations();
    CompactAnnotationNode firstAnnotation;
    if (annotations != null && !annotations.isEmpty() && (firstAnnotation = field.getVisibleAnnotations().get(0)) != null) {
      for (Map.Entry<String, Object> entry : firstAnnotation.getAnnotations().entrySet()) {
        port.put(entry.getKey(), entry.getValue());
      }
    }

    return port;
  }

  private void putFieldDescription(CompactFieldNode field, JSONObject port, TypeGraphVertex tgv) throws JSONException
  {
    OperatorClassInfo oci = classInfo.get(tgv.typeName);
    if (oci != null) {
      String fieldDesc = oci.fields.get(field.getName());
      if (fieldDesc != null) {
        port.put("description", fieldDesc);
        return;
      }
    }

    for (TypeGraphVertex ancestor : tgv.getAncestors()) {
      putFieldDescription(field, port, ancestor);
    }
  }

  private JSONArray enrichProperties(String operatorClass, JSONArray properties) throws JSONException
  {
    JSONArray result = new JSONArray();
    for (int i = 0; i < properties.length(); i++) {
      JSONObject propJ = properties.getJSONObject(i);
      String propName = WordUtils.capitalize(propJ.getString("name"));
      String getPrefix = (propJ.getString("type").equals("boolean") || propJ.getString("type").equals("java.lang.Boolean")) ? "is" : "get";
      String setPrefix = "set";
      OperatorClassInfo oci = getOperatorClassWithGetterSetter(operatorClass, setPrefix + propName, getPrefix + propName);
      if (oci == null) {
        result.put(propJ);
        continue;
      }
      MethodInfo setterInfo = oci.setMethods.get(setPrefix + propName);
      MethodInfo getterInfo = oci.getMethods.get(getPrefix + propName);

      if ((getterInfo != null && getterInfo.omitFromUI) || (setterInfo != null && setterInfo.omitFromUI)) {
        continue;
      }
      if (setterInfo != null) {
        addTagsToProperties(setterInfo, propJ);
      } else if (getterInfo != null) {
        addTagsToProperties(getterInfo, propJ);
      }
      result.put(propJ);
    }
    return result;
  }

  private OperatorClassInfo getOperatorClassWithGetterSetter(String operatorClass, String setterName, String getterName)
  {
    TypeGraphVertex tgv = typeGraph.getTypeGraphVertex(operatorClass);
    return getOperatorClassWithGetterSetter(tgv, setterName, getterName);
  }

  private OperatorClassInfo getOperatorClassWithGetterSetter(TypeGraphVertex tgv, String setterName, String getterName)
  {
    OperatorClassInfo oci = classInfo.get(tgv.typeName);
    if (oci != null && (oci.getMethods.containsKey(getterName) || oci.setMethods.containsKey(setterName))) {
      return oci;
    } else {
      if (tgv.getAncestors() != null) {
        for (TypeGraphVertex ancestor : tgv.getAncestors()) {
          return getOperatorClassWithGetterSetter(ancestor, setterName, getterName);
        }
      }
    }

    return null;
  }

  private void addTagsToProperties(MethodInfo mi, JSONObject propJ) throws JSONException
  {
    //create description object. description tag enables the visual tools to display description of keys/values
    //of a map property, items of a list, properties within a complex type.
    JSONObject descriptionObj = new JSONObject();
    if (mi.comment != null) {
      descriptionObj.put("$", mi.comment);
    }
    for (Map.Entry<String, String> descEntry : mi.descriptions.entrySet()) {
      descriptionObj.put(descEntry.getKey(), descEntry.getValue());
    }
    if (descriptionObj.length() > 0) {
      propJ.put("descriptions", descriptionObj);
    }

    //create useSchema object. useSchema tag is added to enable visual tools to be able to render a text field
    //as a dropdown with choices populated from the schema attached to the port.
    JSONObject useSchemaObj = new JSONObject();
    for (Map.Entry<String, String> useSchemaEntry : mi.useSchemas.entrySet()) {
      useSchemaObj.put(useSchemaEntry.getKey(), useSchemaEntry.getValue());
    }
    if (useSchemaObj.length() > 0) {
      propJ.put("useSchema", useSchemaObj);
    }
  }

  public JSONObject describeClass(String clazzName) throws Exception
  {
    return describeClassByASM(clazzName);
  }


  public JSONObject describeClassByASM(String clazzName) throws Exception
  {
    return typeGraph.describeClass(clazzName);
  }



  public JSONObject describeClass(Class<?> clazz) throws Exception
  {
    JSONObject desc = new JSONObject();
    desc.put("name", clazz.getName());
    if (clazz.isEnum()) {
      @SuppressWarnings("unchecked")
      Class<Enum<?>> enumClass = (Class<Enum<?>>)clazz;
      ArrayList<String> enumNames = Lists.newArrayList();
      for (Enum<?> e : enumClass.getEnumConstants()) {
        enumNames.add(e.name());
      }
      desc.put("enum", enumNames);
    }
    UI_TYPE ui_type = UI_TYPE.getEnumFor(clazz);
    if (ui_type != null) {
      desc.put("uiType", ui_type.getName());
    }
    desc.put("properties", getClassProperties(clazz, 0));
    return desc;
  }


  private static String getDocName(String clazz)
  {
    return clazz.replace('.', '/').replace('$', '.') + ".html";
  }

  private JSONArray getClassProperties(Class<?> clazz, int level) throws IntrospectionException
  {
    JSONArray arr = new JSONArray();
    TypeDiscoverer td = new TypeDiscoverer();
    try {
      for (PropertyDescriptor pd : Introspector.getBeanInfo(clazz).getPropertyDescriptors()) {
        Method readMethod = pd.getReadMethod();
        if (readMethod != null) {
          if (readMethod.getDeclaringClass() == java.lang.Enum.class) {
            // skip getDeclaringClass
            continue;
          } else if ("class".equals(pd.getName())) {
            // skip getClass
            continue;
          }
        } else {
          // yields com.datatorrent.api.Context on JDK6 and com.datatorrent.api.Context.OperatorContext with JDK7
          if ("up".equals(pd.getName()) && com.datatorrent.api.Context.class.isAssignableFrom(pd.getPropertyType())) {
            continue;
          }
        }
        //LOG.info("name: " + pd.getName() + " type: " + pd.getPropertyType());

        Class<?> propertyType = pd.getPropertyType();
        if (propertyType != null) {
          JSONObject propertyObj = new JSONObject();
          propertyObj.put("name", pd.getName());
          propertyObj.put("canGet", readMethod != null);
          propertyObj.put("canSet", pd.getWriteMethod() != null);
          if (readMethod != null) {
            for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
              OperatorClassInfo oci = classInfo.get(c.getName());
              if (oci != null) {
                MethodInfo getMethodInfo = oci.getMethods.get(readMethod.getName());
                if (getMethodInfo != null) {
                  addTagsToProperties(getMethodInfo, propertyObj);
                  break;
                }
              }
            }
            // type can be a type symbol or parameterized type
            td.setTypeArguments(clazz, readMethod.getGenericReturnType(), propertyObj);
          } else {
            if (pd.getWriteMethod() != null) {
              td.setTypeArguments(clazz, pd.getWriteMethod().getGenericParameterTypes()[0], propertyObj);
            }
          }
          //if (!propertyType.isPrimitive() && !propertyType.isEnum() && !propertyType.isArray() && !propertyType
          // .getName().startsWith("java.lang") && level < MAX_PROPERTY_LEVELS) {
          //  propertyObj.put("properties", getClassProperties(propertyType, level + 1));
          //}
          arr.put(propertyObj);
        }
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return arr;
  }

  private static final Pattern CAPS = Pattern.compile("([A-Z\\d][^A-Z\\d]*)");

  private static String decamelizeClassName(String className)
  {
    Matcher match = CAPS.matcher(className);
    StringBuilder deCameled = new StringBuilder();
    while (match.find()) {
      if (deCameled.length() == 0) {
        deCameled.append(match.group());
      } else {
        deCameled.append(" ");
        deCameled.append(match.group().toLowerCase());
      }
    }
    return deCameled.toString();
  }

  /**
   * Enrich portClassHier with class/interface names that map to a list of parent classes/interfaces.
   * For any class encountered, find its parents too.<br/>
   * Also find the port types which have assignable schema classes.
   *
   * @param oper                       Operator to work on
   * @param portClassHierarchy         In-Out param that contains a mapping of class/interface to its parents
   * @param portTypesWithSchemaClasses Json that will contain all the ports which have any schema classes.
   */
  public void buildAdditionalPortInfo(JSONObject oper, JSONObject portClassHierarchy, JSONObject portTypesWithSchemaClasses)
  {
    try {
      JSONArray ports = oper.getJSONArray(OperatorDiscoverer.PORT_TYPE_INFO_KEY);
      for (int i = 0; i < ports.length(); i++) {
        JSONObject port = ports.getJSONObject(i);

        String portType = port.optString("type");
        if (portType == null) {
          //skipping if port type is null
          continue;
        }

        if (typeGraph.size() == 0) {
          buildTypeGraph();
        }

        try {
          //building port class hierarchy
          LinkedList<String> queue = new LinkedList<>();
          queue.add(portType);
          while (!queue.isEmpty()) {
            String currentType = queue.remove();
            if (portClassHierarchy.has(currentType)) {
              //already present in the json so we skip.
              continue;
            }
            List<String> immediateParents = typeGraph.getParents(currentType);
            if (immediateParents == null) {
              portClassHierarchy.put(currentType, new ArrayList<String>());
              continue;
            }
            portClassHierarchy.put(currentType, immediateParents);
            queue.addAll(immediateParents);
          }
        } catch (JSONException e) {
          LOG.warn("building port type hierarchy {}", portType, e);
        }

        //finding port types with schema classes
        if (portTypesWithSchemaClasses.has(portType)) {
          //already present in the json so skipping
          continue;
        }
        if (portType.equals("byte") || portType.equals("short") || portType.equals("char") || portType.equals("int")
            || portType.equals("long") || portType.equals("float") || portType.equals("double")
            || portType.equals("java.lang.String") || portType.equals("java.lang.Object")) {
          //ignoring primitives, strings and object types as this information is needed only for complex types.
          continue;
        }
        if (port.has("typeArgs")) {
          //ignoring any type with generics
          continue;
        }
        boolean hasSchemaClasses = false;
        List<String> instantiableDescendants = typeGraph.getInstantiableDescendants(portType);
        if (instantiableDescendants != null) {
          for (String descendant : instantiableDescendants) {
            try {
              if (typeGraph.isInstantiableBean(descendant)) {
                hasSchemaClasses = true;
                break;
              }
            } catch (JSONException ex) {
              LOG.warn("checking descendant is instantiable {}", descendant);
            }
          }
        }
        portTypesWithSchemaClasses.put(portType, hasSchemaClasses);
      }
    } catch (JSONException e) {
      // should not reach this
      LOG.error("JSON Exception {}", e);
      throw new RuntimeException(e);
    }
  }

  public JSONArray getDescendants(String fullClassName)
  {
    if (typeGraph.size() == 0) {
      buildTypeGraph();
    }
    return new JSONArray(typeGraph.getDescendants(fullClassName));
  }


  public TypeGraph getTypeGraph()
  {
    return typeGraph;
  }

}
