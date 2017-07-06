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
package org.apache.apex.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.CodeSource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
/**
 * @since 3.6.0
 */
public class JarHelper
{
  private static final Logger logger = LoggerFactory.getLogger(JarHelper.class);
  private static final String APEX_DEPENDENCIES = "apex-dependencies";

  private final Map<URL, String> sourceToJar = new HashMap<>();

  public static String createJar(String prefix, File dir, boolean deleteOnExit) throws IOException
  {
    if (!dir.exists() || !dir.isDirectory()) {
      throw new IllegalArgumentException(String.format("dir %s must be an existing directory.", dir));
    }
    File temp = File.createTempFile(prefix, ".jar");
    if (deleteOnExit) {
      temp.deleteOnExit();
    }
    new JarCreator(temp).createJar(dir);
    return temp.getAbsolutePath();
  }

  public String getJar(Class<?> jarClass, boolean makeJarFromFolder)
  {
    String jar = null;
    final CodeSource codeSource = jarClass.getProtectionDomain().getCodeSource();
    if (codeSource != null) {
      URL location = codeSource.getLocation();
      jar = sourceToJar.get(location);
      if (jar == null) {
        // don't create jar file from folders multiple times
        if ("jar".equals(location.getProtocol())) {
          try {
            location = ((JarURLConnection)location.openConnection()).getJarFileURL();
          } catch (IOException e) {
            throw new AssertionError("Cannot resolve jar file for " + jarClass, e);
          }
        }
        if ("file".equals(location.getProtocol())) {
          jar = location.getFile();
          final File dir = new File(jar);
          if (dir.isDirectory()) {
            if (!makeJarFromFolder) {
              throw new AssertionError("Cannot resolve jar file for " + jarClass + ". URL " + location);
            }
            try {
              jar = createJar("apex-", dir, false);
            } catch (IOException e) {
              throw new AssertionError("Cannot resolve jar file for " + jarClass + ". URL " + location, e);
            }
          }
        } else {
          throw new AssertionError("Cannot resolve jar file for " + jarClass + ". URL " + location);
        }
        sourceToJar.put(location, jar);
        logger.debug("added sourceLocation {} as {}", location, jar);
      }
      if (jar == null) {
        throw new AssertionError("Cannot resolve jar file for " + jarClass);
      }
    }
    return jar;
  }

  public String getJar(Class<?> jarClass)
  {
    return getJar(jarClass, true);
  }

  /**
   * Returns a full path to the jar-file that contains the given class and all full paths to dependent jar-files
   * that are defined in the property "apex-dependencies" of the manifest of the root jar-file.
   * If the class is an independent file the method makes jar file from the folder that contains the class
   * @param jarClass Class
   * @param makeJarFromFolder True if the method should make jar from folder that contains the independent class
   * @param addJarDependencies True if the method should include dependent jar files
   * @return Set of names of the jar-files
   */
  public Set<String> getJars(Class<?> jarClass, boolean makeJarFromFolder, boolean addJarDependencies)
  {
    String jar = getJar(jarClass, makeJarFromFolder);
    Set<String> set = new HashSet<>();
    if (jar != null) {
      set.add(jar);
      if (addJarDependencies) {
        try {
          getDependentJarsFromManifest(new JarFile(jar), set);
        } catch (IOException ex) {
          logger.warn("Cannot open Jar-file {}", jar);
        }
      }
    }
    return set;
  }

  /**
   * Returns a full path to the jar-file that contains the given class and all full paths to dependent jar-files
   * that are defined in the property "apex-dependencies" of the manifest of the root jar-file.
   * If the class is an independent file the method makes jar file from the folder that contains the class
   * @param jarClass Class
   * @return Set of names of the jar-files
   */
  public Set<String> getJars(Class<?> jarClass)
  {
    return getJars(jarClass, true, true);
  }

  /**
   * Adds dependent jar-files from manifest to the target list of jar-files
   * @param jarFile Jar file
   * @param set Set of target jar-files
   * @throws IOException
   */
  public void getDependentJarsFromManifest(JarFile jarFile, Set<String> set) throws IOException
  {
    String value = jarFile.getManifest().getMainAttributes().getValue(APEX_DEPENDENCIES);
    if (!StringUtils.isEmpty(value)) {
      Path folderPath = Paths.get(jarFile.getName()).getParent();
      for (String jar : value.split(",")) {
        File file = folderPath.resolve(jar).toFile();
        if (file.exists()) {
          set.add(file.getPath());
          logger.debug("The file {} was added as a dependent of the jar {}", file.getPath(), jarFile.getName());
        } else {
          logger.warn("The dependent file {} of the jar {} does not exist", file.getPath(), jarFile.getName());
        }
      }
    }
  }

  private static class JarCreator
  {

    private final JarOutputStream jos;

    private JarCreator(File file) throws IOException
    {
      jos = new JarOutputStream(new FileOutputStream(file));
    }

    private void createJar(File dir) throws IOException
    {
      try {
        File manifestFile = new File(dir, JarFile.MANIFEST_NAME);
        if (!manifestFile.exists()) {
          jos.putNextEntry(new JarEntry(JarFile.MANIFEST_NAME));
          new Manifest().write(jos);
          jos.closeEntry();
        } else {
          addEntry(manifestFile, JarFile.MANIFEST_NAME);
        }
        final Path root = dir.toPath();
        Files.walkFileTree(root,
            new SimpleFileVisitor<Path>()
            {
              String relativePath;

              @Override
              public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
              {
                relativePath = root.relativize(dir).toString();
                if (!relativePath.isEmpty()) {
                  if (!relativePath.endsWith("/")) {
                    relativePath += "/";
                  }
                  addEntry(dir.toFile(), relativePath);
                }
                return super.preVisitDirectory(dir, attrs);
              }

              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
              {
                String name = relativePath + file.getFileName().toString();
                if (!JarFile.MANIFEST_NAME.equals(name)) {
                  addEntry(file.toFile(), relativePath + file.getFileName().toString());
                }
                return super.visitFile(file, attrs);
              }

              @Override
              public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
              {
                relativePath = root.relativize(dir.getParent()).toString();
                if (!relativePath.isEmpty() && !relativePath.endsWith("/")) {
                  relativePath += "/";
                }
                return super.postVisitDirectory(dir, exc);
              }
            }
        );
      } finally {
        jos.close();
      }
    }

    private void addEntry(File file, String name) throws IOException
    {
      final JarEntry ze = new JarEntry(name);
      ze.setTime(file.lastModified());
      jos.putNextEntry(ze);
      if (file.isFile()) {
        try (final FileInputStream input = new FileInputStream(file)) {
          IOUtils.copy(input, jos);
        }
      }
      jos.closeEntry();
    }
  }
}
