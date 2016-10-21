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
package com.datatorrent.stram.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.stram.client.ClassPathResolvers.ManifestResolver;
import com.datatorrent.stram.client.ClassPathResolvers.MavenResolver;
import com.datatorrent.stram.client.ClassPathResolvers.Resolver;

public class ClassPathResolverTest
{
  @Test
  public void testResolverConfig() throws Exception
  {
    ClassPathResolvers cpr = new ClassPathResolvers();
    List<Resolver> resolvers = cpr.createResolvers("mvn:,manifest:/baseDir");
    Assert.assertEquals(2, resolvers.size());
    Assert.assertEquals(MavenResolver.class, resolvers.get(0).getClass());
    Assert.assertEquals(ManifestResolver.class, resolvers.get(1).getClass());
    Assert.assertEquals("/baseDir", ((ManifestResolver)resolvers.get(1)).baseDir.getPath());
  }

  @Test
  public void testManifestClassPathResolver() throws Exception
  {
    String artifactId = "hadoop-common";
    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/META-INF/maven/org.apache.hadoop/" + artifactId + "/pom.properties"));
    String version = properties.getProperty("version");

    String jarPath = "org/apache/hadoop/" + artifactId + "/" + version + "/" + artifactId + "-" + version + ".jar";

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, jarPath);

    File testFile = new File("target/resolverTest.jar");

    FileOutputStream stream = new FileOutputStream(testFile);
    JarOutputStream out = new JarOutputStream(stream, manifest);
    out.putNextEntry(new JarEntry("foo/bar"));
    out.write("Hello World".getBytes());
    out.closeEntry();
    out.finish();
    out.close();

    JarFile jarFile = new JarFile(testFile);
    ClassPathResolvers.JarFileContext jfc = new ClassPathResolvers.JarFileContext(jarFile, new StringWriter());

    ClassPathResolvers cpr = new ClassPathResolvers();
    String baseDir = System.getProperty("localRepository", System.getProperty("user.home") + "/.m2/repository");
    List<ClassPathResolvers.Resolver> resolvers = cpr.createResolvers(ClassPathResolvers.SCHEME_MANIFEST + ":" + baseDir);

    Assert.assertEquals(1, resolvers.size());
    resolvers.get(0).resolve(jfc);

    jarFile.close();

    Assert.assertEquals("number path components " + jfc.urls, 1, jfc.urls.size());
    URL first = jfc.urls.iterator().next();
    Assert.assertEquals("first url", baseDir + "/" + jarPath, first.getPath());
  }

}
