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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.datatorrent.stram.webapp.TypeGraph.TypeGraphSerializer;


/**
 * This class keeps a precomputed index(prototype) of type graph of all classes in jdk and gateway classpath
 *
 * @since 2.1.0
 */
public class TypeGraphFactory
{
  private static final byte[] preComputeGraph;

  private static final Logger LOG = LoggerFactory.getLogger(TypeGraphFactory.class);

  private static TypeGraph tg = null;


  // statically initialize the precomputed type graph out of classes in jdk and jars in current classpath

  static {
    LOG.debug("Pre compute the type graph out of classes in jdk and jars in current classpath");
    final Set<String> pathsToScan = new HashSet<>();

    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(":");
    for (String path: paths) {
      pathsToScan.add(path);
    }

    String javahome = System.getProperty("java.home");
    String jdkJar = javahome + "/lib/rt.jar";
    pathsToScan.add(jdkJar);

    tg = new TypeGraph();


    for (String path : pathsToScan) {
      try {
        File f = new File(path);
        if (!f.exists() || !f.getName().endsWith("jar")) {
          continue;
        }
        JarFile jar = new JarFile(path);
        try {
          java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
          while (entriesEnum.hasMoreElements()) {
            java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
            if (!jarEntry.isDirectory() && jarEntry.getName().endsWith(".class")) {
              tg.addNode(jarEntry, jar);
            }
          }

        } finally {
          jar.close();
        }
      } catch (IOException ex) {
        LOG.warn("Some error happens when parsing the file {}", path, ex);
      }
    }

    Kryo kryo = new Kryo();
    TypeGraphSerializer tgs = new TypeGraphSerializer();
    kryo.register(TypeGraph.class, tgs);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 * 1024 * 20);
    Output output = new Output(baos);
    kryo.writeObject(output, tg);
    output.close();
    preComputeGraph = baos.toByteArray();
    LOG.warn("The size of precomputed type graph is {} KB", preComputeGraph.length / 1024);
  }


  public static TypeGraph createTypeGraphProtoType()
  {
    Input input = null;
    try {
      input = new Input(new ByteArrayInputStream(preComputeGraph));
      Kryo kryo = new Kryo();
      TypeGraphSerializer tgs = new TypeGraphSerializer();
      kryo.register(TypeGraph.class, tgs);
      return kryo.readObject(input, TypeGraph.class);
    } finally {
      IOUtils.closeQuietly(input);
    }
  }

}
