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

import com.datatorrent.api.Operator;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.stram.util.ObjectMapperFactory;
import com.datatorrent.stram.webapp.TypeDiscoverer.UI_TYPE;
import com.datatorrent.stram.webapp.TypeGraph.TypeGraphVertex;
import com.datatorrent.stram.webapp.asm.CompactAnnotationNode;
import com.datatorrent.stram.webapp.asm.CompactFieldNode;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.beans.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

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
  private final List<String> pathsToScan = new ArrayList<String>();
  private final ClassLoader classLoader;
  private static final String DT_OPERATOR_DOCLINK_PREFIX = "https://www.datatorrent.com/docs/apidocs/index.html";
  public static final String PORT_TYPE_INFO_KEY = "portTypeInfo";
  private final TypeGraph typeGraph = TypeGraphFactory.createTypeGraphProtoType();

  private static final String USE_SCHEMA_TAG = "@useSchema";
  private static final String DESCRIPTION_TAG = "@description";
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+?");

  private static final String SCHEMA_REQUIRED_KEY = "schemaRequired";

  private final Map<String, OperatorClassInfo> classInfo = new HashMap<String, OperatorClassInfo>();

  private static class OperatorClassInfo {
    String comment;
    final Map<String, String> tags = new HashMap<String, String>();
    final Map<String, MethodInfo> getMethods = Maps.newHashMap();
    final Map<String, MethodInfo> setMethods = Maps.newHashMap();
    final Set<String> invisibleGetSetMethods = new HashSet<String>();
    final Map<String, String> fields = new HashMap<String, String>();
  }

  private static class MethodInfo
  {
    Map<String, String> descriptions = Maps.newHashMap();
    Map<String, String> useSchemas = Maps.newHashMap();
    String comment;
  }

  private class JavadocSAXHandler extends DefaultHandler {

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
      }
      else if (qName.equalsIgnoreCase("comment")) {
        comment = new StringBuilder();
      }
      else if (qName.equalsIgnoreCase("tag")) {
        if (oci != null) {
          String tagName = attributes.getValue("name");
          String tagText = attributes.getValue("text").trim();
          if (methodName != null) {
            boolean lGetterCheck = isGetter(methodName);
            boolean lSetterCheck = !lGetterCheck && isSetter(methodName);

            if (lGetterCheck || lSetterCheck) {
              if ("@omitFromUI".equals(tagName)) {
                oci.invisibleGetSetMethods.add(methodName);
              } else if (DESCRIPTION_TAG.equals(tagName)) {
                addTagToMethod(lGetterCheck ? oci.getMethods : oci.setMethods, tagText, true);
              } else if (USE_SCHEMA_TAG.equals(tagName)) {
                addTagToMethod(lGetterCheck ? oci.getMethods : oci.setMethods, tagText, false);
              }
            }
//            if ("@return".equals(tagName) && isGetter(methodName)) {
//              oci.getMethods.put(methodName, tagText);
//            }
            //do nothing
          }
          else if (fieldName != null) {
            // do nothing
          }
          else {
            oci.tags.put(tagName, tagText);
          }
        }
      }
      else if (qName.equalsIgnoreCase("field")) {
        fieldName = attributes.getValue("name");
      }
      else if (qName.equalsIgnoreCase("method")) {
        methodName = attributes.getValue("name");
      }
    }

    private void addTagToMethod(Map<String, MethodInfo> methods, String tagText, boolean isDescription)
    {
      MethodInfo mi = methods.get(methodName);
      if (mi == null) {
        mi = new MethodInfo();
        methods.put(methodName, mi);
      }
      String[] tagParts = Iterables.toArray(Splitter.on(WHITESPACE_PATTERN).trimResults().omitEmptyStrings().
        limit(2).split(tagText), String.class);
      if (tagParts.length == 2) {
        if (isDescription) {
          mi.descriptions.put(tagParts[0], tagParts[1]);
        } else {
          mi.useSchemas.put(tagParts[0], tagParts[1]);
        }
      }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      if (qName.equalsIgnoreCase("class")) {
        classInfo.put(className, oci);
        className = null;
        oci = null;
      }
      else if (qName.equalsIgnoreCase("comment") && oci != null) {
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
        }
        else if (fieldName != null) {
          oci.fields.put(fieldName, comment.toString());
        }
        else {
          oci.comment = comment.toString();
        }
        comment = null;
      }
      else if (qName.equalsIgnoreCase("field")) {
        fieldName = null;
      }
      else if (qName.equalsIgnoreCase("method")) {
        methodName = null;
      }
    }

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
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
      }
      catch (MalformedURLException ex) {
        throw new RuntimeException(ex);
      }
    }
    classLoader = new URLClassLoader(urls, ClassLoader.getSystemClassLoader());
  }

  private void loadOperatorClass()
  {
    buildTypeGraph();
    operatorClassNames =  typeGraph.getAllDTInstantiableOperators();
  }

  @SuppressWarnings("unchecked")
  public void addDefaultValue(String className, JSONObject oper) throws Exception
  {
    ObjectMapper defaultValueMapper = ObjectMapperFactory.getOperatorValueSerializer();
    Class<? extends Operator> clazz = (Class<? extends Operator>) classLoader.loadClass(className);
    if (clazz != null) {
      Operator operIns = clazz.newInstance();
      String s = defaultValueMapper.writeValueAsString(operIns);
      oper.put("defaultValue", new JSONObject(s).get(className));
    }
  }

  public void buildTypeGraph()
  {
    Map<String, JarFile> openJarFiles = new HashMap<String, JarFile>();
    Map<String, File> openClassFiles = new HashMap<String, File>();
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
              java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
              if (!jarEntry.isDirectory() && jarEntry.getName().endsWith("-javadoc.xml")) {
                try {
                  processJavadocXml(jar.getInputStream(jarEntry));
                  // break;
                } catch (Exception ex) {
                  LOG.warn("Cannot process javadoc {} : ", jarEntry.getName(), ex);
                }
              } else if (!jarEntry.isDirectory() && jarEntry.getName().endsWith(".class")) {
                typeGraph.addNode(jarEntry, jar);
              }
            }
          }
        } catch (IOException ex) {
          LOG.warn("Cannot process file {}", f, ex);
        }
      }

      typeGraph.trim();

      typeGraph.updatePortTypeInfoInTypeGraph(openJarFiles, openClassFiles);
    }
   finally {
      for (Entry<String, JarFile> entry : openJarFiles.entrySet()) {
        try {
          entry.getValue().close();
        } catch (IOException e) {
          DTThrowable.wrapIfChecked(e);
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
      loadOperatorClass();
    }
    if (parent == null) {
      parent = Operator.class.getName();
    } else {
      if (!typeGraph.isAncestor(Operator.class.getName(), parent)) {
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

    if (searchTerm == null && parent == Operator.class.getName()) {
      return filteredClass;
    }

    if (searchTerm != null) {
      searchTerm = searchTerm.toLowerCase();
    }

    Set<String> result = new HashSet<String>();
    for (String clazz : filteredClass) {
      if (parent == Operator.class.getName() || typeGraph.isAncestor(parent, clazz)) {
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
      loadOperatorClass();
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
          }
          else if (clazz.startsWith("com.datatorrent.lib.") ||
                  clazz.startsWith("com.datatorrent.contrib.")) {
            response.put("doclink", DT_OPERATOR_DOCLINK_PREFIX  + "?" + getDocName(clazz));
          }
        }
      }
      catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
      return response;
    }
    else {
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
      if(oci == null) {
        result.put(propJ);
        continue;
      }
      if (oci.invisibleGetSetMethods.contains(getPrefix + propName) || oci.invisibleGetSetMethods.contains(setPrefix + propName)) {
        continue;
      }
      MethodInfo methodInfo = oci.setMethods.get(setPrefix + propName);
      methodInfo = methodInfo == null ? oci.getMethods.get(getPrefix + propName) : methodInfo;
      if (methodInfo != null) {
        addTagsToProperties(methodInfo, propJ);
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
    if(ui_type!=null){
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
            //if (!propertyType.isPrimitive() && !propertyType.isEnum() && !propertyType.isArray() && !propertyType.getName().startsWith("java.lang") && level < MAX_PROPERTY_LEVELS) {
            //  propertyObj.put("properties", getClassProperties(propertyType, level + 1));
            //}
            arr.put(propertyObj);
          }
      }
    }
    catch (JSONException ex) {
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
      }
      else {
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
          LinkedList<String> queue = Lists.newLinkedList();
          queue.add(portType);
          while (!queue.isEmpty()) {
            String currentType = queue.remove();
            if (portClassHierarchy.has(currentType)) {
              //already present in the json so we skip.
              continue;
            }
            List<String> immediateParents = typeGraph.getParents(currentType);
            if (immediateParents == null) {
              portClassHierarchy.put(currentType, Lists.<String>newArrayList());
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
    if(typeGraph.size()==0){
      buildTypeGraph();
    }
    return new JSONArray(typeGraph.getDescendants(fullClassName));
  }


  public TypeGraph getTypeGraph()
  {
    return typeGraph;
  }

}
