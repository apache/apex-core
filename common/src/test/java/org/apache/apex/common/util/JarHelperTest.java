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

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class JarHelperTest
{
  private static final Logger logger = LoggerFactory.getLogger(JarHelperTest.class);

  private static final String file = "file";
  private static final byte[] data = "data".getBytes();
  private static final String dir = "dir/";
  private static final String META = "META-INF/";
  private static final String version = "1.0";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void createJar() throws Exception
  {
    JarFile jar = new JarFile(JarHelper.createJar("apex", folder.getRoot(), true));
    logger.debug("Created jar {} with MANIFEST only.", jar.getName());
    assertNotNull("MANIFEST exists", jar.getManifest());
    assertNull(jar.getEntry(file));
    assertNull(jar.getEntry(dir));
    jar.close();

    Files.write(folder.newFile(file).toPath(), data);
    folder.newFolder(dir);
    jar = new JarFile(JarHelper.createJar("apex", folder.getRoot(), true));
    logger.debug("Created jar {} with a file and a directory.", jar.getName());
    assertNotNull("MANIFEST exists", jar.getManifest());
    ZipEntry entry = jar.getEntry(file);
    assertNotNull(entry);
    assertFalse(entry.isDirectory());
    byte[] data = new byte[JarHelperTest.data.length];
    jar.getInputStream(entry).read(data);
    assertArrayEquals(data, JarHelperTest.data);
    assertTrue(jar.getEntry(dir).isDirectory());
    jar.close();

    folder.newFolder(META);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, version);
    manifest.write(new FileOutputStream(folder.newFile(JarFile.MANIFEST_NAME)));
    jar = new JarFile(JarHelper.createJar(JarHelperTest.class.getSimpleName(), folder.getRoot(), true));
    logger.debug("Created jar {} with a file, a directory and a MANIFEST.", jar.getName());
    assertEquals("MANIFEST version", jar.getManifest().getMainAttributes().getValue(Attributes.Name.MANIFEST_VERSION), version);
    entry = jar.getEntry(file);
    assertNotNull(entry);
    assertFalse(entry.isDirectory());
    jar.getInputStream(entry).read(data);
    assertArrayEquals(data, JarHelperTest.data);
    assertTrue(jar.getEntry(dir).isDirectory());
    jar.close();
  }

  @Test
  public void getJar() throws Exception
  {
    final JarHelper jarHelper = new JarHelper();

    assertNull("System jar is null", jarHelper.getJar(Class.class));

    String jar = jarHelper.getJar(JarHelper.class);
    assertNotNull("JarHelper jar is not null", jar);
    assertSame(jar, jarHelper.getJar(JarHelper.class));

    jar = jarHelper.getJar(JarHelperTest.class);
    assertNotNull("JarHelperTest jar is not null", jar);
    assertSame(jar, jarHelper.getJar(JarHelperTest.class));
  }
}
