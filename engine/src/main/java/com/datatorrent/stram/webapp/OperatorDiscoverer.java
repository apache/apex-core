/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.*;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  //private final String packagePrefix; // The reason why we need this is that if we scan the entire class path, it's very likely that we end up with out of permgen memory
  private static final int MAX_PROPERTY_LEVELS = 5;

  public OperatorDiscoverer()
  {
    //this.packagePrefix = packagePrefix;
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
    //this.packagePrefix = packagePrefix;
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
          while (entriesEnum.hasMoreElements()) {
            java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
            if (!jarEntry.isDirectory()) {
              String entryName = jarEntry.getName();
              if (entryName.endsWith(".class")) {
                final String className = entryName.replace('/', '.').substring(0, entryName.length() - 6);
                /*
                 if (!className.startsWith(packagePrefix)) {
                 continue;
                 }
                 */
                LOG.debug("Looking at class {} jar {}", className, jarFile);
                try {
                  Class<?> clazz = classLoader.loadClass(className);
                  if (isInstantiableOperatorClass(clazz)) {
                    LOG.debug("Adding class {} as an operator", clazz.getName());
                    operatorClasses.add((Class<? extends Operator>)clazz);
                  }
                }
                catch (ClassNotFoundException ex) {
                  LOG.warn("Class not found: {}", className);
                }
                catch (NoClassDefFoundError ex) {
                  LOG.warn("Class definition not found: {}", className);
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
          // TBD: add other search fields here, e.g. descriptions, categories, tags
          if (clazz.getName().toLowerCase().contains(searchTerm)) {
            result.add(clazz);
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

  public static JSONObject describeOperator(Class<? extends Operator> clazz) throws IntrospectionException
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
            propertyObj.put("canGet", pd.getReadMethod() != null);
            propertyObj.put("canSet", pd.getWriteMethod() != null);
            propertyObj.put("description", pd.getShortDescription());
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

}
