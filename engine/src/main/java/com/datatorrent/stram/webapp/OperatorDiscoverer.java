/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.*;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
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
  private final String packagePrefix; // The reason why we need this is that if we scan the entire class path, it's very likely that we end up with out of permgen memory

  public OperatorDiscoverer(String packagePrefix)
  {
    this.packagePrefix = packagePrefix;
    classLoader = ClassLoader.getSystemClassLoader();
    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(":");
    for (String path: paths) {
      if (path.startsWith("./") && path.endsWith(".jar")) {
        pathsToScan.add(path);
      }
    }
  }

  public OperatorDiscoverer(String packagePrefix, String[] jars)
  {
    this.packagePrefix = packagePrefix;
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
                if (!className.startsWith(packagePrefix)) {
                  continue;
                }
                //LOG.debug("Looking at class {} jar {}", className, jarFile);
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
      parentClass = classLoader.loadClass(parent);
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
