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
package com.datatorrent.stram.webapp.asm;

import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.xbean.asm5.ClassReader;

public class FastClassIndexReaderTest
{
  private static final Logger LOG = LoggerFactory.getLogger(FastClassIndexReaderTest.class);

  @Test
  public void testIndexReader() throws IOException
  {
    String javahome = System.getProperty("java.home");
    String jdkJar = javahome + "/lib/rt.jar";
    JarFile jar = new JarFile(jdkJar);
    java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
    while (entriesEnum.hasMoreElements()) {
      JarEntry jarEntry = entriesEnum.nextElement();
      if (jarEntry.getName().endsWith("class")) {
        InputStream ins = jar.getInputStream(jarEntry);
        ClassReader classReader = new ClassReader(ins);
        ClassNodeType classN = new ClassNodeType();
        classReader.accept(classN, ClassReader.SKIP_CODE);
        CompactClassNode ccn = CompactUtil.compactClassNode(classN);
        ins.close();

        ins = jar.getInputStream(jarEntry);
        FastClassIndexReader fastClassIndexReader = new FastClassIndexReader(ins);
        Assert.assertEquals("The wrong class is " + classN.name, classN.name, fastClassIndexReader.getName());
        Assert.assertEquals("The wrong class is " + classN.name, classN.superName, fastClassIndexReader.getSuperName());
        Assert.assertEquals("The wrong class is " + classN.name, !ASMUtil.isAbstract(classN.access) && ASMUtil.isPublic(classN.access) && ccn.getDefaultConstructor() != null, fastClassIndexReader.isInstantiable());
        Assert.assertArrayEquals("The wrong class is " + classN.name, classN.interfaces.toArray(), fastClassIndexReader.getInterfaces());

      }
    }
  }

  @Test
  public void testPerformance() throws Exception
  {

    String javahome = System.getProperty("java.home");
    String jdkJar = javahome + "/lib/rt.jar";
    JarFile jar = new JarFile(jdkJar);
    java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
    long time = System.currentTimeMillis();
    while (entriesEnum.hasMoreElements()) {
      JarEntry jarEntry = entriesEnum.nextElement();
      if (jarEntry.getName().endsWith("class")) {
        InputStream ins = jar.getInputStream(jarEntry);
        //FastClassSignatureReader fastClassSignatureReader = new FastClassSignatureReader(ins);
        ClassReader classReader = new ClassReader(ins);
        ClassNodeType classN = new ClassNodeType();
        classReader.accept(classN, ClassReader.SKIP_CODE);
        CompactClassNode ccn = CompactUtil.compactClassNode(classN);
        ins.close();
      }
    }

    LOG.info("The time to scan jdk using ASM ClassReader {} ", System.currentTimeMillis() - time);

    jar.close();

    jar = new JarFile(jdkJar);
    entriesEnum = jar.entries();
    time = System.currentTimeMillis();
    while (entriesEnum.hasMoreElements()) {
      JarEntry jarEntry = entriesEnum.nextElement();
      if (jarEntry.getName().endsWith("class")) {
        InputStream ins = jar.getInputStream(jarEntry);
        FastClassIndexReader fastClassIndexReader = new FastClassIndexReader(ins);
        ins.close();
      }
    }

    jar.close();

    LOG.info("The time to scan jdk using FastClassIndexReader {} ", System.currentTimeMillis() - time);

  }
}
