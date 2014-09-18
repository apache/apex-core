/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.annotation.*;
import java.beans.*;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.*;
import org.apache.commons.lang3.StringUtils;
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
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class OperatorDiscoverer
{
  private static class ClassComparator implements Comparator<Class<?>> {

    @Override
    public int compare(Class<?> a, Class<?> b)
    {
      return a.getName().compareTo(b.getName());
    }

  }
  private final Set<Class<? extends Operator>> operatorClasses = new TreeSet<Class<? extends Operator>>(new ClassComparator());
  private static final Logger LOG = LoggerFactory.getLogger(OperatorDiscoverer.class);
  private final List<String> pathsToScan = new ArrayList<String>();
  private final ClassLoader classLoader;
  private final String[] packagePrefixes; // The reason why we need this is that if we scan the entire class path, it's very likely that we end up with out of permgen memory
  private static final int MAX_PROPERTY_LEVELS = 5;

  private final Map<String, OperatorClassInfo> classInfo = new HashMap<String, OperatorClassInfo>();

  private static class OperatorClassInfo {
    String comment;
    final Map<String, String> tags = new HashMap<String, String>();
  }

  private class JavadocSAXHandler extends DefaultHandler {

    private String className = null;
    private OperatorClassInfo oci = null;
    private StringBuilder comment;

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
          String tagText = attributes.getValue("text");
          oci.tags.put(tagName, tagText);
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
        oci.comment = comment.toString();
        comment = null;
      }
    }

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
      if (comment != null) {
        comment.append(ch, start, length);
      }
    }
  }

  public OperatorDiscoverer(String[] packagePrefixes)
  {
    this.packagePrefixes = packagePrefixes;
    classLoader = ClassLoader.getSystemClassLoader();
    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(":");
    for (String path: paths) {
      if (path.startsWith("./") && path.endsWith(".jar")) {
        pathsToScan.add(path);
      }
    }
  }

  public OperatorDiscoverer(String[] packagePrefixes, String[] jars)
  {
    this.packagePrefixes = packagePrefixes;
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

  private boolean isEligibleClass(String className)
  {
    for (String packagePrefix : packagePrefixes) {
      if (className.startsWith(packagePrefix)) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private void init()
  {
    for (String jarFile : pathsToScan) {
      try {
        JarFile jar = new JarFile(jarFile);
        try {
          java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
          while (entriesEnum.hasMoreElements()) {
            java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
            if (!jarEntry.isDirectory()) {
              String entryName = jarEntry.getName();
              if (entryName.endsWith(".class")) {
                final String className = entryName.replace('/', '.').substring(0, entryName.length() - 6);
                if (isEligibleClass(className)) {
                  LOG.debug("Looking at class {} jar {}", className, jarFile);
                  try {
                    Class<?> clazz = classLoader.loadClass(className);
                    if (isInstantiableOperatorClass(clazz)) {
                      LOG.debug("Adding class {} as an operator", clazz.getName());
                      operatorClasses.add((Class<? extends Operator>)clazz);
                    }
                  }
                  catch (Throwable ex) {
                    LOG.warn("Class cannot be loaded: {} (error was {})", className, ex.getMessage());
                  }
                }
              }
              else if (entryName.endsWith("-javadoc.xml")) {
                try {
                  processJavadocXml(jar.getInputStream(jarEntry));
                }
                catch (Exception ex) {
                  LOG.warn("Cannot process javadoc xml: ", ex);
                }
              }
            }
          }
        }
        finally {
          jar.close();
        }
      }
      catch (IOException ex) {
      }
    }
  }

  private void processJavadocXml(InputStream is) throws ParserConfigurationException, SAXException, IOException
  {
    SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
    saxParserFactory.newSAXParser().parse(is, new JavadocSAXHandler());
  }

  public static boolean isInstantiableOperatorClass(Class<?> clazz)
  {
    int modifiers = clazz.getModifiers();
    return !Modifier.isAbstract(modifiers) && !Modifier.isInterface(modifiers) && Operator.class.isAssignableFrom(clazz);
  }

  public Set<Class<? extends Operator>> getOperatorClasses(String parent, String searchTerm) throws ClassNotFoundException
  {
    if (operatorClasses.isEmpty()) {
      init();
    }
    Class<?> parentClass;
    if (parent == null) {
      parentClass = Operator.class;
    }
    else {
      parentClass = classLoader.loadClass(parent);
      if (!Operator.class.isAssignableFrom(parentClass)) {
        throw new IllegalArgumentException("Argument must be a subclass of Operator class");
      }
    }
    if (searchTerm == null && parentClass == Operator.class) {
      return Collections.unmodifiableSet(operatorClasses);
    }
    if (searchTerm != null) {
      searchTerm = searchTerm.toLowerCase();
    }
    Set<Class<? extends Operator>> result = new HashSet<Class<? extends Operator>>();
    for (Class<? extends Operator> clazz : operatorClasses) {
      if (parentClass.isAssignableFrom(clazz)) {
        if (searchTerm == null) {
          result.add(clazz);
        }
        else {
          if (clazz.getName().toLowerCase().contains(searchTerm)) {
            result.add(clazz);
          }
          else {
            OperatorClassInfo oci = classInfo.get(clazz.getName());
            if (oci != null) {
              if (oci.comment != null && oci.comment.toLowerCase().contains(searchTerm)) {
                result.add(clazz);
              }
              else {
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
    if (operatorClasses.isEmpty()) {
      init();
    }

    Class<?> clazz = classLoader.loadClass(className);

    if (!Operator.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Argument must be a subclass of Operator class");
    }
    return (Class<? extends Operator>)clazz;
  }

  public JSONObject describeOperator(Class<? extends Operator> clazz) throws IntrospectionException
  {
    if (OperatorDiscoverer.isInstantiableOperatorClass(clazz)) {
      JSONObject response = new JSONObject();
      JSONArray inputPorts = new JSONArray();
      JSONArray outputPorts = new JSONArray();
      JSONArray properties = OperatorDiscoverer.getClassProperties(clazz);

      Field[] fields = clazz.getFields();
      Arrays.sort(fields, new Comparator<Field>()
          {
            @Override
            public int compare(Field a, Field b)
            {
              return a.getName().compareTo(b.getName());
            }

      });
      try {
        for (Field field : fields) {
          InputPortFieldAnnotation inputAnnotation = field.getAnnotation(InputPortFieldAnnotation.class);
          if (inputAnnotation != null) {
            JSONObject inputPort = new JSONObject();
            inputPort.put("name", inputAnnotation.name());
            inputPort.put("optional", inputAnnotation.optional());
            inputPort.put("displayName", inputAnnotation.displayName());
            inputPort.put("description", inputAnnotation.description());
            inputPorts.put(inputPort);
            continue;
          }
          else if (InputPort.class.isAssignableFrom(field.getType())) {
            JSONObject inputPort = new JSONObject();
            inputPort.put("name", field.getName());
            inputPort.put("optional", false); // input port that is not annotated is default to be non-optional
            inputPorts.put(inputPort);
            continue;
          }
          OutputPortFieldAnnotation outputAnnotation = field.getAnnotation(OutputPortFieldAnnotation.class);
          if (outputAnnotation != null) {
            JSONObject outputPort = new JSONObject();
            outputPort.put("name", outputAnnotation.name());
            outputPort.put("optional", outputAnnotation.optional());
            outputPort.put("error", outputAnnotation.error());
            outputPort.put("displayName", outputAnnotation.displayName());
            outputPort.put("description", outputAnnotation.description());
            outputPorts.put(outputPort);
            //continue;
          }
          else if (OutputPort.class.isAssignableFrom(field.getType())) {
            JSONObject outputPort = new JSONObject();
            outputPort.put("name", field.getName());
            outputPort.put("optional", true); // output port that is not annotated is default to be optional
            outputPorts.put(outputPort);
            //continue;
          }
        }

        response.put("name", clazz.getName());
        response.put("properties", properties);
        response.put("inputPorts", inputPorts);
        response.put("outputPorts", outputPorts);

        OperatorClassInfo oci = classInfo.get(clazz.getName());
        if (oci != null) {
          if (oci.comment != null) {
            String[] descriptions = oci.comment.split("\\.\\s", 2);
            if (descriptions.length > 0) {
              response.put("shortDesc", descriptions[0]);
            }
            if (descriptions.length > 1) {
              response.put("longDesc", descriptions[1]);
            }
          }
          response.put("category", oci.tags.get("@category"));
          String displayName = oci.tags.get("@displayName");
          if (displayName == null) {
            displayName = decamelizeClassName(clazz.getSimpleName());
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

  public static JSONArray getClassProperties(Class<?> clazz) throws IntrospectionException
  {
    return getClassProperties(clazz, 0);
  }

  private static JSONArray getClassProperties(Class<?> clazz, int level) throws IntrospectionException
  {
    JSONArray arr = new JSONArray();
    try {
      for (PropertyDescriptor pd : Introspector.getBeanInfo(clazz).getPropertyDescriptors()) {
        if (!pd.getName().equals("class") && (!(pd.getName().equals("up") && pd.getPropertyType().equals(com.datatorrent.api.Context.class)))) {
          Class<?> propertyType = pd.getPropertyType();
          if (propertyType != null) {
            JSONObject propertyObj = new JSONObject();
            propertyObj.put("name", pd.getName());
            Method readMethod = pd.getReadMethod();
            propertyObj.put("canGet", readMethod != null);
            propertyObj.put("canSet", pd.getWriteMethod() != null);
            if (readMethod != null) {
              PropertyAnnotation pa = readMethod.getAnnotation(PropertyAnnotation.class);
              if (pa != null) {
                propertyObj.put("description", pa.description());
                propertyObj.put("displayName", pa.displayName());
              }
            }
            propertyObj.put("type", propertyType.getName());
            if (!propertyType.isPrimitive() && !propertyType.isEnum() && !propertyType.isArray() && !propertyType.getName().startsWith("java.lang") && level < MAX_PROPERTY_LEVELS) {
              propertyObj.put("properties", getClassProperties(propertyType, level + 1));
            }
            arr.put(propertyObj);
          }
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
}
