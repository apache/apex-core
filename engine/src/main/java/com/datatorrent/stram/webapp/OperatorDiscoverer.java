/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>OperatorDiscoverer class.</p>
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
      return String.CASE_INSENSITIVE_ORDER.compare(a.getName(), b.getName());
    }

  }
  private Set<Class<? extends Operator>> operatorClasses = new TreeSet<Class<? extends Operator>>(new ClassComparator());
  private static final Logger LOG = LoggerFactory.getLogger(OperatorDiscoverer.class);

  private void init()
  {
    List<String> pathsToScan = new ArrayList<String>();
    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(":");
    for (String path : paths) {
      if (path.startsWith("./") && path.endsWith(".jar")) {
        pathsToScan.add(path);
      }
    }

    for (String jarFile : pathsToScan) {
      try {
        JarFile jar = new JarFile(jarFile);

        java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
        while (entriesEnum.hasMoreElements()) {
          java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
          if (!jarEntry.isDirectory()) {
            String entryName = jarEntry.getName();
            if (entryName.endsWith(".class")) {
              final String className = entryName.replace('/', '.').substring(0, entryName.length() - 6);
              try {
                Class<?> clazz = Class.forName(className);
                if (isInstantiableOperatorClass(clazz)) {
                  LOG.info("Adding class {} as an operator", clazz.getName());
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
        jar.close();
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

  public Set<Class<? extends Operator>> getOperatorClasses(String parent) throws ClassNotFoundException
  {
    if (operatorClasses.isEmpty()) {
      init();
    }
    Class<?> parentClass;
    if (parent == null) {
      parentClass = Operator.class;
    }
    else {
      parentClass = Class.forName(parent);
      if (!Operator.class.isAssignableFrom(parentClass)) {
        throw new IllegalArgumentException("Argument must be a subclass of Operator class");
      }
    }
    if (parentClass == Operator.class) {
      return Collections.unmodifiableSet(operatorClasses);
    }
    Set<Class<? extends Operator>> result = new HashSet<Class<? extends Operator>>();
    for (Class<? extends Operator> clazz : operatorClasses) {
      if (parentClass.isAssignableFrom(clazz)) {
        result.add(clazz);
      }
    }
    return result;
  }

  List<Class<? extends Operator>> getActionOperatorClasses()
  {
    if (operatorClasses.isEmpty()) {
      init();
    }

    List<Class<? extends Operator>> result = new ArrayList<Class<? extends Operator>>();
    for (Class<? extends Operator> clazz : operatorClasses) {
      Field[] fields = clazz.getFields();
      for (Field field : fields) {
        if (InputPort.class.isAssignableFrom(field.getType())) {
          result.add(clazz);
          break;
        }
      }
    }
    return result;
  }

}
