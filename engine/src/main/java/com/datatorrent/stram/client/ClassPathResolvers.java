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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

/**
 * <p>ClassPathResolvers class.</p>
 *
 * @since 0.9.4
 */
public class ClassPathResolvers
{
  public static final String SCHEME_MANIFEST = "manifest";
  public static final String SCHEME_MVN = "mvn";

  /**
   * Information about the jar file that is collected initially and provided to each resolver so that it does not need
   * to be retrieved repeatedly.
   */
  public static class JarFileContext
  {
    public JarFileContext(JarFile file, StringWriter consoleOutput)
    {
      this.jarFile = file;
      this.consoleOutput = consoleOutput;
    }

    File filePath;
    File cacheDir;
    final JarFile jarFile;
    final StringWriter consoleOutput;
    JarEntry pomEntry;
    LinkedHashSet<URL> urls = new LinkedHashSet<>();
  }

  interface Resolver
  {
    /**
     * Resolve classpath for given jar file.
     * @param jfc
     * @throws IOException
     */
    void resolve(JarFileContext jfc) throws IOException;
  }

  /**
   * Resolver that matches jar file path from manifest against repository layout.
   * Performs exact match for each path component relative to repository root.
   */
  public static class ManifestResolver implements Resolver
  {
    private static final Logger LOG = LoggerFactory.getLogger(ManifestResolver.class);
    public static final Attributes.Name ATTR_NAME = Attributes.Name.CLASS_PATH;
    public final File baseDir;

    public ManifestResolver(File baseDir)
    {
      this.baseDir = baseDir;
    }

    @Override
    public void resolve(JarFileContext jfc) throws IOException
    {
      String jarClasspath = jfc.jarFile.getManifest().getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
      if (jarClasspath != null) {
        LOG.debug("Using manifest attribute {} to resolve dependencies", Attributes.Name.CLASS_PATH);
        String[] jars = jarClasspath.split(" ");
        for (String jar : jars) {
          File f = new File(baseDir, jar);
          if (f.exists()) {
            jfc.urls.add(f.getAbsoluteFile().toURI().toURL());
          }
        }
      }
    }
  }

  /**
   * Resolver that calls Maven to determine the class path based on pom.xml embedded in the jar file.
   */
  public static class MavenResolver implements Resolver
  {
    private static final Logger LOG = LoggerFactory.getLogger(MavenResolver.class);
    private String userHome;

    @Override
    public void resolve(JarFileContext jfc) throws IOException
    {
      File baseDir = jfc.cacheDir;
      baseDir.mkdirs();

      File pomCrcFile = new File(baseDir, "pom.xml.crc");
      File cpFile = new File(baseDir, "mvn-classpath");
      long pomCrc = 0;
      String cp = null;

      // read crc and classpath file, if it exists
      // (we won't run mvn again if pom didn't change)
      if (cpFile.exists()) {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(pomCrcFile))) {
          pomCrc = dis.readLong();
          cp = FileUtils.readFileToString(cpFile, "UTF-8");
        } catch (Exception e) {
          LOG.error("Cannot read CRC from {}", pomCrcFile);
        }
      }

      // TODO: cache based on application jar checksum
      FileUtils.deleteDirectory(baseDir);

      if (jfc.pomEntry != null) {
        File pomDst = new File(baseDir, "pom.xml");
        FileUtils.copyInputStreamToFile(jfc.jarFile.getInputStream(jfc.pomEntry), pomDst);
        if (pomCrc != jfc.pomEntry.getCrc()) {
          LOG.info("CRC of " + jfc.pomEntry.getName() + " changed, invalidating cached classpath.");
          cp = null;
          pomCrc = jfc.pomEntry.getCrc();
        }
      }

      File pomFile = new File(baseDir, "pom.xml");
      if (pomFile.exists()) {
        if (cp == null) {
          // try to generate dependency classpath
          cp = generateClassPathFromPom(pomFile, cpFile, jfc);
        }
        if (cp != null) {
          try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(pomCrcFile))) {
            dos.writeLong(pomCrc);
          }
          // wasn't the path already written to the file by mvn?
          FileUtils.writeStringToFile(cpFile, cp, false);
          String[] pathList = org.apache.commons.lang.StringUtils.splitByWholeSeparator(cp, ":");
          for (String path : pathList) {
            jfc.urls.add(new File(path).toURI().toURL());
          }
        }
      }
    }

    private String generateClassPathFromPom(File pomFile, File cpFile, JarFileContext jfc) throws IOException
    {
      String cp;
      LOG.info("Generating classpath via mvn from " + pomFile);
      LOG.info("java.home: " + System.getProperty("java.home"));

      String dt_home = StringUtils.isEmpty(userHome) ? "" : (" -Duser.home=" + userHome);
      String cmd = "mvn dependency:build-classpath" + dt_home + " -q -Dmdep.outputFile=" + cpFile.getAbsolutePath() + " -f " + pomFile;
      LOG.debug("Executing: {}", cmd);
      Process p = Runtime.getRuntime().exec(new String[] {"bash", "-c", cmd});
      ProcessWatcher pw = new ProcessWatcher(p);
      InputStream output = p.getInputStream();
      while (!pw.isFinished()) {
        IOUtils.copy(output, jfc.consoleOutput);
      }
      if (pw.rc != 0) {
        throw new RuntimeException("Failed to run: " + cmd + " (exit code " + pw.rc + ")" + "\n" + jfc.consoleOutput.toString());
      }
      cp = FileUtils.readFileToString(cpFile);
      return cp;
    }

    /**
     *
     * Starts a command and waits for it to complete
     * <p>
     * <br>
     *
     */
    public static class ProcessWatcher implements Runnable
    {
      private final Process p;
      private volatile boolean finished = false;
      private volatile int rc;

      @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
      public ProcessWatcher(Process p)
      {
        this.p = p;
        new Thread(this).start();
      }

      public boolean isFinished()
      {
        return finished;
      }

      @Override
      public void run()
      {
        try {
          rc = p.waitFor();
        } catch (Exception e) {
          // ignore
        }
        finished = true;
      }
    }

  }

  /**
   * Parse the resolver configuration
   * @param resolverConfig
   * @return
   */
  public List<Resolver> createResolvers(String resolverConfig)
  {
    String[] specs = resolverConfig.split(",");
    List<Resolver> resolvers = new ArrayList<>(specs.length);
    for (String s : specs) {
      s = s.trim();
      String[] comps = s.split(":");
      if (comps.length == 0) {
        throw new IllegalArgumentException(String.format("Invalid resolver spec %s in %s", s, resolverConfig));
      }
      if (SCHEME_MANIFEST.equals(comps[0])) {
        if (comps.length < 2) {
          throw new IllegalArgumentException(String.format("Missing repository path in manifest resolver spec %s in %s", s, resolverConfig));
        }
        File baseDir = new File(comps[1]);
        resolvers.add(new ManifestResolver(baseDir));
      } else if (SCHEME_MVN.equals(comps[0])) {
        MavenResolver mvnr = new MavenResolver();
        if (comps.length > 1) {
          mvnr.userHome = comps[1];
        }
        resolvers.add(mvnr);
      } else {
        throw new NotImplementedException("Unknown resolver scheme " + comps[0]);
      }
    }
    return resolvers;
  }

}
