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
import java.io.IOException;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.lf5.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.stram.DAGPropertiesBuilder;
import com.malhartech.stram.StramClient;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.stram.StramUtils;


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
  private final List<AppConfig> configurationList = new ArrayList<AppConfig>();
  private LinkedHashSet<URL> launchDependencies;

  /**
   *
   * Starts a command and waits for it to complete<p>
   * <br>
   *
   */
  public static class ProcessWatcher implements Runnable {

    private final Process p;
    private volatile boolean finished = false;
    private volatile int rc;

    public ProcessWatcher(Process p) {
        this.p = p;
        new Thread(this).start();
    }

    public boolean isFinished() {
        return finished;
    }

    @Override
    public void run() {
        try {
            rc = p.waitFor();
        } catch (Exception e) {}
        finished = true;
    }
  }


  public static interface AppConfig {
      DAG createApp(Configuration conf);
      String getName();
  }

  public static class PropertyFileAppConfig implements AppConfig {
    final File propertyFile;

    public PropertyFileAppConfig(File file) {
      this.propertyFile = file;
    }

    @Override
    public DAG createApp(Configuration conf) {
      try {
        return DAGPropertiesBuilder.create(conf, propertyFile.getAbsolutePath());
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to load: " + this, e);
      }
    }

    @Override
    public String getName() {
      return propertyFile.getName();
    }

  }


  public StramAppLauncher(File appJarFile) throws Exception {
    this.jarFile = appJarFile;
    init();
  }

  private void init() throws Exception {

    File baseDir =  StramClientUtils.getSettingsRootDir();
    baseDir = new File(baseDir, jarFile.getName());
    baseDir.mkdirs();

    File pomCrcFile = new File(baseDir, "pom.xml.crc");
    File cpFile = new File(baseDir, "mvn-classpath");
    long pomCrc = 0;
    String cp = null;

    // read crc and classpath file, if it exists
    // (we won't run mvn again if pom didn't change)
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
    List<String> classFileNames = new ArrayList<String>();

    java.util.Enumeration<JarEntry> entriesEnum = jar.entries();
    while (entriesEnum.hasMoreElements()) {
        java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
        if (!jarEntry.isDirectory()) {
          if (jarEntry.getName().endsWith("pom.xml")) {
            File pomDst = new File(baseDir, "pom.xml");
            FileUtils.copyInputStreamToFile(jar.getInputStream(jarEntry), pomDst);
            if (pomCrc != jarEntry.getCrc()) {
              LOG.info("CRC of " + jarEntry.getName() + " changed, invalidating cached classpath.");
              cp = null;
              pomCrc = jarEntry.getCrc();
            }
          } else if (jarEntry.getName().endsWith(".app.properties")) {
            File targetFile = new File(baseDir, jarEntry.getName());
            FileUtils.copyInputStreamToFile(jar.getInputStream(jarEntry), targetFile);
            configurationList.add(new PropertyFileAppConfig(targetFile));
          } else if (jarEntry.getName().endsWith(".class")) {
            classFileNames.add(jarEntry.getName());
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

        String malhar_home = System.getenv("MALHAR_HOME");
        if (malhar_home != null && !malhar_home.isEmpty()) {
          malhar_home = " -Duser.home=" + malhar_home;
        }
        else {
          malhar_home = "";
        }
        String cmd = "mvn dependency:build-classpath" + malhar_home + " -Dmdep.outputFile=" + cpFile.getAbsolutePath() + " -f " + pomFile;

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

    // we have the classpath dependencies, scan for java configurations
    findAppConfigClasses(classFileNames);

  }

  /**
   * Scan the application jar file entries for configuration classes.
   * This needs to occur in a class loader with access to the application dependencies.
   */
  private void findAppConfigClasses(List<String> classFileNames) {
    URLClassLoader cl = URLClassLoader.newInstance(launchDependencies.toArray(new URL[launchDependencies.size()]));
    for (final String classFileName : classFileNames) {
      final String className = classFileName.replace('/', '.').substring(0, classFileName.length() - 6);
      try {
        Class<?> clazz = cl.loadClass(className);
        if (ApplicationFactory.class.isAssignableFrom(clazz)) {
          configurationList.add(new AppConfig() {

            @Override
            public String getName() {
              return classFileName;
            }

            @Override
            public DAG createApp(Configuration conf) {
              // load class from current context class loader
              Class<? extends ApplicationFactory> c = StramUtils.classForName(className, ApplicationFactory.class);
              ApplicationFactory f = StramUtils.newInstance(c);
              return f.getApplication(conf);
            }
          });
        }
      } catch (Throwable e) { // java.lang.NoClassDefFoundError
        LOG.error("Unable to load class: " + className + " " + e);
      }
    }
  }

  private static Configuration getConfig(String launchMode) {
    Configuration conf = new Configuration(false);
    StramClientUtils.addStramResources(conf);
    // user settings
    File cfgResource = new File(StramClientUtils.getSettingsRootDir(), StramClientUtils.STRAM_SITE_XML_FILE);
    if (cfgResource.exists()) {
      LOG.info("Loading settings: " + cfgResource.toURI());
      conf.addResource(new Path(cfgResource.toURI()));
    }
    //File appDir = new File(StramClientUtils.getSettingsRootDir(), jarFile.getName());
    //cfgResource = new File(appDir, StramClientUtils.STRAM_SITE_XML_FILE);
    //if (cfgResource.exists()) {
    //  LOG.info("Loading settings from: " + cfgResource.toURI());
    //  conf.addResource(new Path(cfgResource.toURI()));
    //}
    conf.set(DAG.STRAM_LAUNCH_MODE, launchMode);
    return conf;
  }

  public static DAG prepareDAG(AppConfig appConfig, String launchMode) {
    Configuration conf = getConfig(launchMode);
    DAG dag = appConfig.createApp(conf);
    dag.getAttributes().attr(DAG.STRAM_APPNAME).setIfAbsent(appConfig.getName());
    // inject external operator configuration
    DAGPropertiesBuilder pb = new DAGPropertiesBuilder();
    pb.addFromConfiguration(conf);
    pb.setOperatorProperties(dag, dag.getAttributes().attr(DAG.STRAM_APPNAME).get());
    return dag;
  }

  /**
   * Run application in-process. Returns only once application completes.
   * @param appConfig
   * @throws Exception
   */
  public void runLocal(AppConfig appConfig) throws Exception {
    // local mode requires custom classes to be resolved through the context class loader
    loadDependencies();
    StramLocalCluster lc = new StramLocalCluster(prepareDAG(appConfig, ApplicationFactory.LAUNCHMODE_LOCAL));
    lc.run();
  }

  public URLClassLoader loadDependencies() {
    URLClassLoader cl = URLClassLoader.newInstance(launchDependencies.toArray(new URL[launchDependencies.size()]));
    Thread.currentThread().setContextClassLoader(cl);
    return cl;
  }

  /**
   * Submit application to the cluster and return the app id.
   * @param appConfig
   * @return ApplicationId
   * @throws Exception
   */
  public ApplicationId launchApp(AppConfig appConfig) throws Exception {

    URLClassLoader cl = loadDependencies();
    //Class<?> loadedClass = cl.loadClass("com.malhartech.example.wordcount.WordCountSerDe");
    //LOG.info("loaded " + loadedClass);

    // below would be needed w/o parent delegation only
    // using parent delegation assumes that stram is in the JVM launch classpath
    Class<?> childClass = cl.loadClass(StramAppLauncher.class.getName());
    Method runApp = childClass.getMethod("runApp", new Class[] {AppConfig.class});
    // TODO: with class loader isolation, pass serialized appConfig to launch loader
    Object appIdStr = runApp.invoke(null, appConfig);
    return ConverterUtils.toApplicationId(""+appIdStr);
  }

  public static String runApp(AppConfig appConfig) throws Exception {
    LOG.info("Launching configuration: {}", appConfig.getName());

    DAG dag = prepareDAG(appConfig, ApplicationFactory.LAUNCHMODE_YARN);
    StramClient client = new StramClient(dag);
    client.startApplication();
    return client.getApplicationReport().getApplicationId().toString();
  }

  public List<AppConfig> getBundledTopologies() {
    return Collections.unmodifiableList(this.configurationList);
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    String jarFileName = "/home/hdev/devel/malhar/stramproto/examples/wordcount/target/wordcount-example-1.0-SNAPSHOT.jar";
    StramAppLauncher appLauncher = new StramAppLauncher(new File(jarFileName));
    if (appLauncher.configurationList.isEmpty()) {
      throw new IllegalArgumentException("jar file does not contain any topology definitions.");
    }
    AppConfig cfg = appLauncher.configurationList.get(0);
    appLauncher.launchApp(cfg);
  }

}
