/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.cli;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.jar.JarEntry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.lf5.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.StramClient;
import com.malhartech.stram.conf.ShipContainingJars;


/**
 * 
 * Launch a streaming application packaged as jar file<p>
 * <br>
 * Parses the jar file for
 * dependency pom and topology files. Dependency resolution is based on the
 * bundled pom.xml (if any) and the application is lauched with a modified
 * client classpath that includes application dependencies so that classes
 * defined in the topology can be loaded and {@link ShipContainingJars}
 * annotations processed. Caching is performed for dependency classpath
 * resolution.<br>
 * <br>
 */

public class StramAppLauncher {
  
  private static final Logger LOG = LoggerFactory.getLogger(StramAppLauncher.class);

  final File jarFile;
  private List<File> topologyList = new ArrayList<File>();
  private LinkedHashSet<URL> launchDependencies;
  
  public static class ProcessWatcher implements Runnable {

    private Process p;
    private volatile boolean finished = false;
    private volatile int rc;
    
    public ProcessWatcher(Process p) {
        this.p = p;
        new Thread(this).start();
    }

    public boolean isFinished() {
        return finished;
    }

    public void run() {
        try {
            rc = p.waitFor();
        } catch (Exception e) {}
        finished = true;
    }
  }  

  public StramAppLauncher(File appJarFile) throws Exception {
    this.jarFile = appJarFile;
    init();
  }
  
  private void init() throws Exception {

    File baseDir =  new File(FileUtils.getUserDirectory(), ".stram");
    baseDir = new File(baseDir, jarFile.getName());
    baseDir.mkdirs();

    File pomCrcFile = new File(baseDir, "pom.xml.crc");
    File cpFile = new File(baseDir, "mvn-classpath");
    long pomCrc = 0;
    String cp = null; 

    // read crc and classpath file, if it exists
    // (we won't run mvn if pom didn't change)
    if (cpFile.exists()) {
      try {
       DataInputStream dis = new DataInputStream(new FileInputStream(pomCrcFile));
       pomCrc = dis.readLong();
       dis.close();
       cp = FileUtils.readFileToString(cpFile, "UTF-8");
      } catch (Exception e) {
        LOG.error("Cannot read CRC from {}", pomCrcFile);
      }
    }
    
 // TODO: cache based on application jar checksum
    FileUtils.deleteDirectory(baseDir);
    
    java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile);
    
    java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
    while (entriesEnum.hasMoreElements()) {
        java.util.jar.JarEntry file = (java.util.jar.JarEntry) entriesEnum.nextElement();
        if (!file.isDirectory()) {
          if (file.getName().endsWith("pom.xml")) {
            File pomDst = new File(baseDir, "pom.xml");
            FileUtils.copyInputStreamToFile(jar.getInputStream(file), pomDst);
            if (pomCrc != file.getCrc()) {
              LOG.info("CRC of " + file.getName() + " changed, invalidating cached classpath.");
              cp = null;
              pomCrc = file.getCrc();
            }
          } else if (file.getName().endsWith(".tplg.properties")) {
            // TODO: handle topology files in subdirs
            File targetFile = new File(baseDir, file.getName());
            FileUtils.copyInputStreamToFile(jar.getInputStream(file), targetFile);
            topologyList.add(targetFile);
          }
        }
    }    
    jar.close();
    
    File pomFile = new File(baseDir, "pom.xml");
    if (pomFile.exists()) {
      if (cp == null) {
        // try to generate dependency classpath
        LOG.info("Generating classpath via mvn from " + pomFile);
        LOG.info("java.home: " + System.getProperty("java.home"));
        String cmd = "mvn dependency:build-classpath -Dmdep.outputFile=" + cpFile.getAbsolutePath() + " -f " + pomFile;
        Process p = Runtime.getRuntime().exec(cmd);
        ProcessWatcher pw = new ProcessWatcher(p);
        InputStream output = p.getInputStream();
        while(!pw.isFinished()) {
            StreamUtils.copy(output, System.out);
        }
        if (pw.rc != 0) {
          throw new RuntimeException("Failed to run: " + cmd + " (exit code " + pw.rc + ")");
        }
        cp = FileUtils.readFileToString(cpFile);
      }
      DataOutputStream dos = new DataOutputStream(new FileOutputStream(pomCrcFile));
      dos.writeLong(pomCrc);
      dos.close();
      FileUtils.writeStringToFile(cpFile, cp, false);
    }

    LinkedHashSet<URL> clUrls = new LinkedHashSet<URL>();

//    // dependencies from parent loader
//    ClassLoader baseCl = StramAppLauncher.class.getClassLoader();
//    if (baseCl instanceof URLClassLoader) {
//      URL[] baseUrls = ((URLClassLoader)baseCl).getURLs();
//      // launch class path takes precedence - add first  
//      clUrls.addAll(Arrays.asList(baseUrls));
//    }
    
    clUrls.add(new URL("jar", "","file:" + jarFile.getAbsolutePath()+"!/"));
    // add the jar dependencies
    if (cp != null) {
      String[] pathList = org.apache.commons.lang.StringUtils.splitByWholeSeparator(cp, ":");
      for (String path : pathList) {
        clUrls.add(new URL("file:" + path));
      }
    }

    for (URL baseURL : clUrls) {
      LOG.debug("Dependency: {}", baseURL);
    }
    
    this.launchDependencies = clUrls;
    
  }

  public ApplicationId launchTopology(File topologyFile) throws Exception {

    URLClassLoader cl = URLClassLoader.newInstance(launchDependencies.toArray(new URL[launchDependencies.size()]));   
    //Class<?> loadedClass = cl.loadClass("com.malhartech.example.wordcount.WordCountSerDe");    
    //LOG.info("loaded " + loadedClass);
    Thread.currentThread().setContextClassLoader(cl);
    // below would be needed w/o parent delegation only
    // using parent delegation assumes that stram is in the JVM launch classpath
    Class<?> childClass = cl.loadClass(StramAppLauncher.class.getName());
    Method runApp = childClass.getMethod("runApp", new Class[] {File.class});
    Object appIdStr = runApp.invoke(null, topologyFile);
    return ConverterUtils.toApplicationId(""+appIdStr);
  }
  
  public static String runApp(File topologyFile) throws Exception {
    LOG.info("Launching topology: {}", topologyFile);

    String[] args = {
        "--topologyProperties",
        topologyFile.getAbsolutePath()
    };
    
    StramClient client = new StramClient();
    boolean initSuccess = client.init(args);
    if (!initSuccess) {
      throw new RuntimeException("Failed to initialize client.");
    }
    client.startApplication();
    return client.getApplicationReport().getApplicationId().toString();
  }

  public List<File> getBundledTopologies() {
    return Collections.unmodifiableList(this.topologyList);
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    String jarFileName = "/home/hdev/devel/malhar/stramproto/examples/wordcount/target/wordcount-example-1.0-SNAPSHOT.jar";
    StramAppLauncher appLauncher = new StramAppLauncher(new File(jarFileName));
    if (appLauncher.topologyList.isEmpty()) {
      throw new IllegalArgumentException("jar file does not contain any topology definitions.");
    }
    File topology = appLauncher.topologyList.get(0);
    appLauncher.launchTopology(topology);
  }
  
}
