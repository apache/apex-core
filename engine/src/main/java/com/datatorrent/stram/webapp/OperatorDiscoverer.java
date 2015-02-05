/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.Unifier;
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
  private static final int MAX_PROPERTY_LEVELS = 5;
  private final String dtOperatorDoclinkPrefix = "https://www.datatorrent.com/docs/apidocs/index.html";
  public static final String PORT_TYPE_INFO_KEY = "portTypeInfo";

  private final Map<String, OperatorClassInfo> classInfo = new HashMap<String, OperatorClassInfo>();

  private static class OperatorClassInfo {
    String comment;
    final Map<String, String> tags = new HashMap<String, String>();
    final Map<String, String> getMethods = new HashMap<String, String>();
    final Map<String, String> fields = new HashMap<String, String>();
  }

  private class JavadocSAXHandler extends DefaultHandler {

    private String className = null;
    private OperatorClassInfo oci = null;
    private StringBuilder comment;
    private String fieldName = null;
    private String methodName = null;
    private final Pattern getterPattern = Pattern.compile("(?:is|get)[A-Z].*");

    private boolean isGetter(String methodName)
    {
      return getterPattern.matcher(methodName).matches();
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
          String tagText = attributes.getValue("text");
          if (methodName != null) {
            if ("@return".equals(tagName) && isGetter(methodName)) {
              oci.getMethods.put(methodName, tagText);
            }
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
    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(":");
    for (String path: paths) {
      if (path.startsWith("./") && path.endsWith(".jar")) {
        pathsToScan.add(path);
      }
    }
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

  @SuppressWarnings("unchecked")
  private void init()
  {
    for (String jarFile : pathsToScan) {
      try {
        JarFile jar = new JarFile(jarFile);
        try {
          java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
          boolean hasJavadocXml = false;
          while (entriesEnum.hasMoreElements()) {
            java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
            if (!jarEntry.isDirectory() && jarEntry.getName().endsWith("-javadoc.xml")) {
              try {
                processJavadocXml(jar.getInputStream(jarEntry));
                hasJavadocXml = true;
                break;
              }
              catch (Exception ex) {
                LOG.warn("Cannot process javadoc xml: ", ex);
              }
            }
          }
          // skip jars that don't have the javadoc xml.  for now, we use this to identify jars that contain operator classes.
          // change this code if there's a better way to identify this in the future.
          // note that if we process all jars, we will likely end up with out of permgen space error because of transitive dependencies.
          if (hasJavadocXml) {
            entriesEnum = jar.entries();
            while (entriesEnum.hasMoreElements()) {
              java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
              if (!jarEntry.isDirectory()) {
                String entryName = jarEntry.getName();
                if (entryName.endsWith(".class")) {
                  final String className = entryName.replace('/', '.').substring(0, entryName.length() - 6);
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
    return !Modifier.isAbstract(modifiers) && !Modifier.isInterface(modifiers) && Operator.class.isAssignableFrom(clazz) && ! Unifier.class.isAssignableFrom(clazz);
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
      JSONArray properties = getClassProperties(clazz);

      TypeDiscoverer td = new TypeDiscoverer();
      JSONArray portTypeInfo = td.getPortTypes(clazz);

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
          if (InputPort.class.isAssignableFrom(field.getType())) {
            JSONObject inputPort = new JSONObject();
            inputPort.put("name", field.getName());

            for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
              OperatorClassInfo oci = classInfo.get(c.getName());
              if (oci != null) {
                String fieldDesc = oci.fields.get(field.getName());
                if (fieldDesc != null) {
                  inputPort.put("description", fieldDesc);
                  break;
                }
              }
            }

            InputPortFieldAnnotation inputAnnotation = field.getAnnotation(InputPortFieldAnnotation.class);
            if (inputAnnotation != null) {
              inputPort.put("optional", inputAnnotation.optional());
            }
            else {
              inputPort.put("optional", false); // input port that is not annotated is default to be not optional
            }

            inputPorts.put(inputPort);
          }
          else if (OutputPort.class.isAssignableFrom(field.getType())) {
            JSONObject outputPort = new JSONObject();
            outputPort.put("name", field.getName());

            for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
              OperatorClassInfo oci = classInfo.get(c.getName());
              if (oci != null) {
                String fieldDesc = oci.fields.get(field.getName());
                if (fieldDesc != null) {
                  outputPort.put("description", fieldDesc);
                  break;
                }
              }
            }

            OutputPortFieldAnnotation outputAnnotation = field.getAnnotation(OutputPortFieldAnnotation.class);
            if (outputAnnotation != null) {
              outputPort.put("optional", outputAnnotation.optional());
              outputPort.put("error", outputAnnotation.error());
            }
            else {
              outputPort.put("optional", true); // output port that is not annotated is default to be optional
              outputPort.put("error", false);
            }

            outputPorts.put(outputPort);
          }
        }

        response.put("name", clazz.getName());
        response.put("properties", properties);
        response.put(PORT_TYPE_INFO_KEY, portTypeInfo);
        response.put("inputPorts", inputPorts);
        response.put("outputPorts", outputPorts);

        OperatorClassInfo oci = classInfo.get(clazz.getName());

        if (oci != null) {
          if (oci.comment != null) {
            String[] descriptions;
            // first look for a <p> tag
            descriptions = oci.comment.split("<p>", 2);
            if (descriptions.length == 0) {
              // if no <p> tag, then look for a blank line
              descriptions = oci.comment.split("\n\n", 2);
            }
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
          String doclink = oci.tags.get("@doclink");
          if (doclink != null) {
            response.put("doclink", doclink + "?" + getDocName(clazz));
          }
          else if (clazz.getName().startsWith("com.datatorrent.lib.") ||
                  clazz.getName().startsWith("com.datatorrent.contrib.")) {
            response.put("doclink", dtOperatorDoclinkPrefix + "?" + getDocName(clazz));
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

  private static String getDocName(Class<?> clazz)
  {
    return clazz.getName().replace('.', '/').replace('$', '.') + ".html";
  }

  private JSONArray getClassProperties(Class<?> clazz) throws IntrospectionException
  {
    return getClassProperties(clazz, 0);
  }

  public JSONArray getClassProperties(Class<?> clazz, int level) throws IntrospectionException
  {
    JSONArray arr = new JSONArray();
    try {
      for (PropertyDescriptor pd : Introspector.getBeanInfo(clazz).getPropertyDescriptors()) {
        if (!"class".equals(pd.getName()) && (!("up".equals(pd.getName()) && pd.getPropertyType().equals(com.datatorrent.api.Context.class)))) {
          Class<?> propertyType = pd.getPropertyType();
          if (propertyType != null) {
            JSONObject propertyObj = new JSONObject();
            propertyObj.put("name", pd.getName());
            Method readMethod = pd.getReadMethod();
            propertyObj.put("canGet", readMethod != null);
            propertyObj.put("canSet", pd.getWriteMethod() != null);
            if (readMethod != null) {
              for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
                OperatorClassInfo oci = classInfo.get(c.getName());
                if (oci != null) {
                  String getMethodDesc = oci.getMethods.get(readMethod.getName());
                  if (getMethodDesc != null) {
                    propertyObj.put("description", oci.getMethods.get(readMethod.getName()));
                    break;
                  }
                }
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

  /**
   * Enrich portClassHier with class/interface names that map to a list of parent classes/interfaces.
   * For any class encountered, find its parents too.
   *
   * @param oper Operator to work on
   * @param portClassHier In-Out param that contains a mapping of class/interface to its parents
   */
  public void buildPortClassHier(JSONObject oper, JSONObject portClassHier) {
    try {
      JSONArray ports = oper.getJSONArray(OperatorDiscoverer.PORT_TYPE_INFO_KEY);
      int num_ports = ports.length();
      for (int i = 0; i < num_ports; i++) {
        JSONObject port = ports.getJSONObject(i);

        String type;
        try {
          type = port.getString("type");
        } catch (JSONException e) {
          // no type key
          continue;
        }

        try {
          // load the port type class
          Class<?> portClazz = classLoader.loadClass(type.replaceAll("\\bclass ", "").replaceAll("\\binterface ", ""));

          // iterate up the class hierarchy to populate the portClassHier map
          while (portClazz != null) {
            ArrayList<String> parents = new ArrayList<String>();

            String portClazzName = portClazz.toString();
            if (portClassHier.has(portClazzName)) {
              // already present in portClassHier, so we can stop
              break;
            }

            // interfaces and Object are at the top of the tree, so we can just put them
            // in portClassHier with empty parents, then move on.
            if (portClazz.isInterface() || portClazzName.equals("java.lang.Object")) {
              portClassHier.put(portClazzName, parents);
              break;
            }

            // look at superclass first
            Class<?> superClazz = portClazz.getSuperclass();
            try {
              String superClazzName = superClazz.toString();
              parents.add(superClazzName);
            } catch (NullPointerException e) {
              LOG.info("Superclass is null for `{}` ({})", portClazz, superClazz);
            }
            // then look at interfaces implemented in this port
            for (Class<?> intf : portClazz.getInterfaces()) {
              String intfName = intf.toString();
              if (!portClassHier.has(intfName)) {
                // add the interface to portClassHier
                portClassHier.put(intfName, new ArrayList<String>());
              }
              parents.add(intfName);
            }

            // now store class=>parents mapping in portClassHier
            portClassHier.put(portClazzName, parents);

            // walk up the hierarchy for the next iteration
            portClazz = superClazz;
          }
        } catch (ClassNotFoundException e) {
          LOG.info("Could not make class from `{}`", type);
        }
      }
    } catch (JSONException e) {
      // should not reach this
      LOG.error("JSON Exception {}", e);
      throw new RuntimeException(e);
    }
  }
}
