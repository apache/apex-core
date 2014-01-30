/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.client;

import java.io.*;
import java.lang.reflect.Modifier;
import java.net.*;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.*;
import java.util.jar.JarEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * Launch a streaming application packaged as jar file
 * <p>
 * Parses the jar file for application resources (implementations of {@link StreamingApplication} or property files per
 * naming convention).<br>
 * Dependency resolution is based on the bundled pom.xml (if any) and the application is launched with a modified client
 * classpath that includes application dependencies so that classes defined in the DAG can be loaded and
 * {@link ShipContainingJars} annotations processed. Caching is performed for dependency classpath resolution.<br>
 * <br>
 *
 * @since 0.3.2
 */
public class StramAppLauncher
{
  public static final String LIBJARS_CONF_KEY_NAME = "tmplibjars";
  public static final String FILES_CONF_KEY_NAME = "tmpfiles";
  public static final String ARCHIVES_CONF_KEY_NAME = "tmparchives";
  private static final Logger LOG = LoggerFactory.getLogger(StramAppLauncher.class);
  private final File jarFile;
  private FileSystem fs;
  private Configuration conf;
  private final LogicalPlanConfiguration propertiesBuilder = new LogicalPlanConfiguration();
  private final List<AppFactory> appResourceList = new ArrayList<AppFactory>();
  private LinkedHashSet<URL> launchDependencies;
  private final StringWriter mvnBuildClasspathOutput = new StringWriter();
  private boolean ignorePom = false;

  private String generateClassPathFromPom(File pomFile, File cpFile) throws IOException
  {
    String cp;
    LOG.info("Generating classpath via mvn from " + pomFile);
    LOG.info("java.home: " + System.getProperty("java.home"));

    String dt_home;
    if (StramClientUtils.DT_HOME != null && !StramClientUtils.DT_HOME.isEmpty()) {
      dt_home = " -Duser.home=" + StramClientUtils.DT_HOME;
    }
    else {
      dt_home = "";
    }
    String cmd = "mvn dependency:build-classpath" + dt_home + " -q -Dmdep.outputFile=" + cpFile.getAbsolutePath() + " -f " + pomFile;
    LOG.debug("Executing: {}", cmd);
    Process p = Runtime.getRuntime().exec(new String[] {"bash", "-c", cmd});
    ProcessWatcher pw = new ProcessWatcher(p);
    InputStream output = p.getInputStream();
    while (!pw.isFinished()) {
      IOUtils.copy(output, mvnBuildClasspathOutput);
    }
    if (pw.rc != 0) {
      throw new RuntimeException("Failed to run: " + cmd + " (exit code " + pw.rc + ")" + "\n" + mvnBuildClasspathOutput.toString());
    }
    cp = FileUtils.readFileToString(cpFile);
    return cp;
  }

  /**
   *
   * Starts a command and waits for it to complete<p>
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
      }
      catch (Exception e) {
      }
      finished = true;
    }

  }

  public static interface AppFactory
  {
    StreamingApplication createApp(Configuration conf);

    String getName();

  }

  public static class PropertyFileAppFactory implements AppFactory
  {
    final File propertyFile;

    public PropertyFileAppFactory(File file)
    {
      this.propertyFile = file;
    }

    @Override
    public StreamingApplication createApp(Configuration conf)
    {
      try {
        return LogicalPlanConfiguration.create(conf, propertyFile.getAbsolutePath());
      }
      catch (IOException e) {
        throw new IllegalArgumentException("Failed to load: " + this, e);
      }
    }

    @Override
    public String getName()
    {
      return propertyFile.getName();
    }

  }

  public StramAppLauncher(File appJarFile) throws Exception
  {
    this(appJarFile, null, false);
  }

  public StramAppLauncher(File appJarFile, Configuration conf, boolean ignorePom) throws Exception
  {
    this.jarFile = appJarFile;
    this.conf = conf;
    this.ignorePom = ignorePom;
    init();
  }

  public StramAppLauncher(FileSystem fs, Path path) throws Exception
  {
    this(fs, path, null, false);
  }

  public StramAppLauncher(FileSystem fs, Path path, Configuration conf, boolean ignorePom) throws Exception
  {
    File jarsDir = new File(StramClientUtils.getSettingsRootDir(), "jars");
    jarsDir.mkdirs();
    File localJarFile = new File(jarsDir, path.getName());
    this.fs = fs;
    fs.copyToLocalFile(path, new Path(localJarFile.getAbsolutePath()));
    this.jarFile = localJarFile;
    this.conf = conf;
    this.ignorePom = ignorePom;
    init();
  }

  public String getMvnBuildClasspathOutput()
  {
    return mvnBuildClasspathOutput.toString();
  }

  private void init() throws Exception
  {

    if (conf == null) {
      conf = getConfig(null, null);
    }
    propertiesBuilder.addFromConfiguration(conf);
    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
    Map<String, String> newEntries = new HashMap<String, String>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (entry.getKey().startsWith("stram.")) {
        String newKey = DAGContext.DT_PREFIX + entry.getKey().substring(6);
        LOG.warn("Configuration property {} is deprecated. Please use {} instead.", entry.getKey(), newKey);
        newEntries.put(newKey, entry.getValue());
      }
      iterator.remove();
    }
    for (Map.Entry<String, String> entry : newEntries.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    File baseDir = StramClientUtils.getSettingsRootDir();
    baseDir = new File(baseDir, jarFile.getName());
    baseDir.mkdirs();

    File pomCrcFile = new File(baseDir, "pom.xml.crc");
    File cpFile = new File(baseDir, "mvn-classpath");
    long pomCrc = 0;
    String cp = null;

    // read crc and classpath file, if it exists
    // (we won't run mvn again if pom didn't change)
    if (!ignorePom && cpFile.exists()) {
      try {
        DataInputStream dis = new DataInputStream(new FileInputStream(pomCrcFile));
        pomCrc = dis.readLong();
        dis.close();
        cp = FileUtils.readFileToString(cpFile, "UTF-8");
      }
      catch (Exception e) {
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
        if (!ignorePom && jarEntry.getName().endsWith("pom.xml")) {
          File pomDst = new File(baseDir, "pom.xml");
          FileUtils.copyInputStreamToFile(jar.getInputStream(jarEntry), pomDst);
          if (pomCrc != jarEntry.getCrc()) {
            LOG.info("CRC of " + jarEntry.getName() + " changed, invalidating cached classpath.");
            cp = null;
            pomCrc = jarEntry.getCrc();
          }
        }
        else if (jarEntry.getName().endsWith(".app.properties")) {
          File targetFile = new File(baseDir, jarEntry.getName());
          FileUtils.copyInputStreamToFile(jar.getInputStream(jarEntry), targetFile);
          appResourceList.add(new PropertyFileAppFactory(targetFile));
        }
        else if (jarEntry.getName().endsWith(".class")) {
          classFileNames.add(jarEntry.getName());
        }
      }
    }
    jar.close();

    if (!ignorePom) {
      File pomFile = new File(baseDir, "pom.xml");
      if (pomFile.exists()) {
        if (cp == null) {
          // try to generate dependency classpath
          cp = generateClassPathFromPom(pomFile, cpFile);
        }
        if (cp != null) {
          DataOutputStream dos = new DataOutputStream(new FileOutputStream(pomCrcFile));
          dos.writeLong(pomCrc);
          dos.close();
          FileUtils.writeStringToFile(cpFile, cp, false);
        }
      }
    }

    LinkedHashSet<URL> clUrls = new LinkedHashSet<URL>();

    // dependencies from parent loader, if classpath can't be found from pom
    if (cp == null) {
      ClassLoader baseCl = StramAppLauncher.class.getClassLoader();
      if (baseCl instanceof URLClassLoader) {
        URL[] baseUrls = ((URLClassLoader)baseCl).getURLs();
        // launch class path takes precedence - add first
        clUrls.addAll(Arrays.asList(baseUrls));
      }
    }

    URL mainJarUrl = new URL("jar", "", "file:" + jarFile.getAbsolutePath() + "!/");
    URLConnection urlConnection = mainJarUrl.openConnection();
    if (urlConnection instanceof JarURLConnection) {
      // JDK6 keeps jar file shared and open as long as the process is running.
      // we want the jar file to be opened on every launch to pick up latest changes
      // http://abondar-howto.blogspot.com/2010/06/howto-unload-jar-files-loaded-by.html
      // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4167874
      ((JarURLConnection)urlConnection).getJarFile().close();
    }

    clUrls.add(mainJarUrl);
    // add the jar dependencies
    if (cp != null) {
      String[] pathList = org.apache.commons.lang.StringUtils.splitByWholeSeparator(cp, ":");
      for (String path : pathList) {
        clUrls.add(new URL("file:" + path));
      }
    }

    String libjars = conf.get(LIBJARS_CONF_KEY_NAME);
    if (libjars != null) {
      for (String libjar : libjars.split(",")) {
        // if hdfs, copy from hdfs to local
        URI uri = new URI(libjar);
        String scheme = uri.getScheme();
        if (scheme == null) {
          clUrls.add(new URL("file:" + libjar));
        }
        else if (scheme.equals("file")) {
          clUrls.add(new URL(libjar));
        }
        else if (scheme.equals("hdfs")) {
          if (fs != null) {
            Path path = new Path(libjar);
            File dependencyJarsDir = new File(StramClientUtils.getSettingsRootDir(), "dependencyJars");
            dependencyJarsDir.mkdirs();
            File localJarFile = new File(dependencyJarsDir, path.getName());
            fs.copyToLocalFile(path, new Path(localJarFile.getAbsolutePath()));
            clUrls.add(new URL("file:" + localJarFile.getAbsolutePath()));
          }
          else {
            throw new NotImplementedException("Jar file needs to be from HDFS also in order for the dependency jars to be in HDFS");
          }
        }
        else {
          throw new NotImplementedException("Scheme '" + scheme + "' in libjars not supported");
        }
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
  private void findAppConfigClasses(List<String> classFileNames)
  {
    URLClassLoader cl = URLClassLoader.newInstance(launchDependencies.toArray(new URL[launchDependencies.size()]));
    for (final String classFileName : classFileNames) {
      final String className = classFileName.replace('/', '.').substring(0, classFileName.length() - 6);
      try {
        Class<?> clazz = cl.loadClass(className);
        if (!Modifier.isAbstract(clazz.getModifiers()) && StreamingApplication.class.isAssignableFrom(clazz)) {
          final AppFactory appConfig = new AppFactory()
          {
            @Override
            public String getName()
            {
              return classFileName;
            }

            @Override
            public StreamingApplication createApp(Configuration conf)
            {
              // load class from current context class loader
              Class<? extends StreamingApplication> c = StramUtils.classForName(className, StreamingApplication.class);
              return StramUtils.newInstance(c);
            }

          };
          appResourceList.add(appConfig);
        }
      }
      catch (Throwable e) { // java.lang.NoClassDefFoundError
        LOG.error("Unable to load class: " + className + " " + e);
      }
    }
  }

  public static Configuration getConfig(String overrideConfFileName, Map<String, String> overrideProperties) throws IOException
  {
    Configuration conf = new Configuration(false);
    StramClientUtils.addStramResources(conf);
    if (overrideConfFileName != null) {
      File overrideConfFile = new File(overrideConfFileName);
      if (overrideConfFile.exists()) {
        LOG.info("Loading settings: " + overrideConfFile.toURI());
        conf.addResource(new Path(overrideConfFile.toURI()));
      }
      else {
        throw new IOException("Problem opening file " + overrideConfFile);
      }
    }
    if (overrideProperties != null) {
      for (Map.Entry<String, String> entry : overrideProperties.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }

  public Map<String, String> getAppAliases()
  {
    return propertiesBuilder.getAppAliases();
  }

  public LogicalPlanConfiguration getLogicalPlanConfiguration()
  {
    return propertiesBuilder;
  }

  public LogicalPlan prepareDAG(AppFactory appConfig)
  {
    LogicalPlan dag = new LogicalPlan();
    StreamingApplication app = appConfig.createApp(conf);
    propertiesBuilder.prepareDAG(dag, app, appConfig.getName(), conf);
    return dag;
  }

  /**
   * Run application in-process. Returns only once application completes.
   *
   * @param appConfig
   * @throws Exception
   */
  public void runLocal(AppFactory appConfig) throws Exception
  {
    // local mode requires custom classes to be resolved through the context class loader
    loadDependencies();
    conf.set(DAG.LAUNCH_MODE, StreamingApplication.LAUNCHMODE_LOCAL);
    StramLocalCluster lc = new StramLocalCluster(prepareDAG(appConfig));
    lc.run();
  }

  public URLClassLoader loadDependencies()
  {
    URLClassLoader cl = URLClassLoader.newInstance(launchDependencies.toArray(new URL[launchDependencies.size()]));
    Thread.currentThread().setContextClassLoader(cl);
    return cl;
  }

  /**
   * Submit application to the cluster and return the app id.
   * Sets the context class loader for application dependencies.
   *
   * @param appConfig
   * @return ApplicationId
   * @throws Exception
   */
  public ApplicationId launchApp(AppFactory appConfig) throws Exception
  {

    loadDependencies();
    conf.set(DAG.LAUNCH_MODE, StreamingApplication.LAUNCHMODE_YARN);
    LogicalPlan dag = prepareDAG(appConfig);
    byte[] licenseBytes = StramClientUtils.getLicense(conf);
    dag.setAttribute(LogicalPlan.LICENSE, Base64.encodeBase64String(licenseBytes)); // TODO: obfuscate license passing
    StramClient client = new StramClient(dag);
    client.setLibJars(conf.get(LIBJARS_CONF_KEY_NAME));
    client.setFiles(conf.get(FILES_CONF_KEY_NAME));
    client.setArchives(conf.get(ARCHIVES_CONF_KEY_NAME));
    client.startApplication();
    return client.getApplicationReport().getApplicationId();
  }

  public List<AppFactory> getBundledTopologies()
  {
    return Collections.unmodifiableList(this.appResourceList);
  }

}
