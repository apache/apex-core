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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.util.StreamingAppFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tools.ant.DirectoryScanner;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.StringCodecs;
import com.datatorrent.stram.client.ClassPathResolvers.JarFileContext;
import com.datatorrent.stram.client.ClassPathResolvers.ManifestResolver;
import com.datatorrent.stram.client.ClassPathResolvers.Resolver;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.security.StramUserLogin;

/**
 * Launch a streaming application packaged as jar file
 * <p>
 * Parses the jar file for application resources (implementations of {@link StreamingApplication} or property files per
 * naming convention).<br>
 * Dependency resolution is based on the bundled pom.xml (if any) and the application is launched with a modified client
 * classpath that includes application dependencies so that classes defined in the DAG can be loaded and
 * <br>
 *
 * @since 0.3.2
 */
public class StramAppLauncher
{
  public static final String CLASSPATH_RESOLVERS_KEY_NAME = StreamingApplication.DT_PREFIX + "classpath.resolvers";
  public static final String LIBJARS_CONF_KEY_NAME = "_apex.libjars";
  public static final String FILES_CONF_KEY_NAME = "_apex.files";
  public static final String ARCHIVES_CONF_KEY_NAME = "_apex.archives";
  public static final String ORIGINAL_APP_ID = "_apex.originalAppId";
  public static final String QUEUE_NAME = "_apex.queueName";
  public static final String TAGS = "_apex.tags";

  private static final Logger LOG = LoggerFactory.getLogger(StramAppLauncher.class);
  private File jarFile;
  private FileSystem fs;
  private String recoveryAppName;
  private final LogicalPlanConfiguration propertiesBuilder;
  private final Configuration conf;
  private final List<AppFactory> appResourceList = new ArrayList<>();
  private LinkedHashSet<URL> launchDependencies;
  private LinkedHashSet<File> deployJars;
  private final StringWriter mvnBuildClasspathOutput = new StringWriter();

  private ClassLoader initialClassLoader;
  private Thread loaderThread;

  public interface AppFactory
  {
    LogicalPlan createApp(LogicalPlanConfiguration conf);

    String getName();

    String getDisplayName();
  }

  public static class PropertyFileAppFactory implements AppFactory
  {
    final File propertyFile;

    public PropertyFileAppFactory(File file)
    {
      this.propertyFile = file;
    }

    @Override
    public LogicalPlan createApp(LogicalPlanConfiguration conf)
    {
      try {
        return conf.createFromProperties(LogicalPlanConfiguration.readProperties(propertyFile.getAbsolutePath()), getName());
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to load: " + this + "\n" + e.getMessage(), e);
      }
    }

    @Override
    public String getName()
    {
      String filename = propertyFile.getName();
      if (filename.endsWith(".properties")) {
        return filename.substring(0, filename.length() - 5);
      } else {
        return filename;
      }
    }

    @Override
    public String getDisplayName()
    {
      return getName();
    }

  }

  public static class JsonFileAppFactory implements AppFactory
  {
    final File jsonFile;
    JSONObject json;

    public JsonFileAppFactory(File file)
    {
      this.jsonFile = file;
      try (InputStream is = new FileInputStream(jsonFile)) {
        StringWriter writer = new StringWriter();
        IOUtils.copy(is, writer);
        json = new JSONObject(writer.toString());
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to load: " + this + "\n" + e.getMessage(), e);
      }
    }

    @Override
    public LogicalPlan createApp(LogicalPlanConfiguration conf)
    {
      try {
        return conf.createFromJson(json, getName());
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to load: " + this + "\n" + e.getMessage(), e);
      }
    }

    @Override
    public String getName()
    {
      String filename = jsonFile.getName();
      if (filename.endsWith(".json")) {
        return filename.substring(0, filename.length() - 5);
      } else {
        return filename;
      }
    }

    @Override
    public String getDisplayName()
    {
      String displayName = json.optString("displayName", null);
      return displayName == null ? getName() : displayName;
    }
  }

  public class RecoveryAppFactory implements AppFactory
  {
    @Override
    public LogicalPlan createApp(LogicalPlanConfiguration conf)
    {
      return conf.createEmptyForRecovery(recoveryAppName);
    }

    @Override
    public String getName()
    {
      return recoveryAppName;
    }

    @Override
    public String getDisplayName()
    {
      return recoveryAppName;
    }
  }

  public StramAppLauncher(File appJarFile, Configuration conf) throws Exception
  {
    this.jarFile = appJarFile;
    this.conf = conf;
    this.propertiesBuilder = new LogicalPlanConfiguration(conf);
    init(this.jarFile.getName());
  }

  public StramAppLauncher(FileSystem fs, Path path, Configuration conf) throws Exception
  {
    File jarsDir = new File(StramClientUtils.getUserDTDirectory(), "jars");
    jarsDir.mkdirs();
    File localJarFile = new File(jarsDir, path.getName());
    this.fs = fs;
    fs.copyToLocalFile(path, new Path(localJarFile.getAbsolutePath()));
    this.jarFile = localJarFile;
    this.conf = conf;
    this.propertiesBuilder = new LogicalPlanConfiguration(conf);
    init(this.jarFile.getName());
  }

  public StramAppLauncher(String name, Configuration conf) throws Exception
  {
    this.propertiesBuilder = new LogicalPlanConfiguration(conf);
    this.conf = conf;
    init(name);
  }

  /**
   * This is for recovering an app without specifying apa or appjar file
   *
   * @throws Exception
   */
  public StramAppLauncher(FileSystem fs, Configuration conf) throws Exception
  {
    this.propertiesBuilder = new LogicalPlanConfiguration(conf);
    this.fs = fs;
    this.conf = conf;
    init();
  }

  public String getMvnBuildClasspathOutput()
  {
    return mvnBuildClasspathOutput.toString();
  }

  private void init() throws Exception
  {
    String originalAppId = propertiesBuilder.conf.get(ORIGINAL_APP_ID);
    if (originalAppId == null) {
      throw new AssertionError("Need original app id if launching without apa or appjar");
    }
    Path appsBasePath = new Path(StramClientUtils.getApexDFSRootDir(fs, conf), StramClientUtils.SUBDIR_APPS);
    Path origAppPath = new Path(appsBasePath, originalAppId);
    StringWriter writer = new StringWriter();
    try (FSDataInputStream in = fs.open(new Path(origAppPath, "meta.json"))) {
      IOUtils.copy(in, writer);
    }
    JSONObject metaJson = new JSONObject(writer.toString());
    String originalLibJars = null;

    // Getting the old libjar dependency is necessary here to construct the class loader because during launch it
    // needs to deserialize the dag to change the serialized state with new app id. This may become unnecessary if we
    // don't rely on object deserialization for changing the app id in the future.
    try {
      JSONObject attributes = metaJson.getJSONObject("attributes");
      originalLibJars = attributes.getString(Context.DAGContext.LIBRARY_JARS.getSimpleName());
      recoveryAppName = attributes.getString(Context.DAGContext.APPLICATION_NAME.getSimpleName());
    } catch (JSONException ex) {
      recoveryAppName = "Recovery App From " + originalAppId;
    }

    LinkedHashSet<URL> clUrls = new LinkedHashSet<>();
    String libjars = propertiesBuilder.conf.get(LIBJARS_CONF_KEY_NAME);

    if (StringUtils.isBlank(libjars)) {
      libjars = originalLibJars;
    } else if (StringUtils.isNotBlank(originalLibJars)) {
      libjars = libjars + "," + originalLibJars;
    }
    propertiesBuilder.conf.set(LIBJARS_CONF_KEY_NAME, libjars);
    processLibJars(libjars, clUrls);

    for (URL baseURL : clUrls) {
      LOG.debug("Dependency: {}", baseURL);
    }

    this.launchDependencies = clUrls;
  }

  private void init(String tmpName) throws Exception
  {
    File baseDir = StramClientUtils.getUserDTDirectory();
    baseDir = new File(new File(baseDir, "appcache"), tmpName);
    baseDir.mkdirs();
    LinkedHashSet<URL> clUrls;
    List<String> classFileNames = new ArrayList<>();

    if (jarFile != null) {
      JarFileContext jfc = new JarFileContext(new java.util.jar.JarFile(jarFile), mvnBuildClasspathOutput);
      jfc.cacheDir = baseDir;

      java.util.Enumeration<JarEntry> entriesEnum = jfc.jarFile.entries();
      while (entriesEnum.hasMoreElements()) {
        java.util.jar.JarEntry jarEntry = entriesEnum.nextElement();
        if (!jarEntry.isDirectory()) {
          if (jarEntry.getName().endsWith("pom.xml")) {
            jfc.pomEntry = jarEntry;
          } else if (jarEntry.getName().endsWith(".app.properties")) {
            File targetFile = new File(baseDir, jarEntry.getName());
            FileUtils.copyInputStreamToFile(jfc.jarFile.getInputStream(jarEntry), targetFile);
            appResourceList.add(new PropertyFileAppFactory(targetFile));
          } else if (jarEntry.getName().endsWith(".class")) {
            classFileNames.add(jarEntry.getName());
          }
        }
      }

      URL mainJarUrl = new URL("jar", "", "file:" + jarFile.getAbsolutePath() + "!/");
      jfc.urls.add(mainJarUrl);

      deployJars = Sets.newLinkedHashSet();
      // add all jar files from same directory
      Collection<File> jarFiles = FileUtils.listFiles(jarFile.getParentFile(), new String[]{"jar"}, false);
      for (File lJarFile : jarFiles) {
        jfc.urls.add(lJarFile.toURI().toURL());
        deployJars.add(lJarFile);
      }

      // resolve dependencies
      List<Resolver> resolvers = Lists.newArrayList();

      String resolverConfig = this.propertiesBuilder.conf.get(CLASSPATH_RESOLVERS_KEY_NAME, null);
      if (!StringUtils.isEmpty(resolverConfig)) {
        resolvers = new ClassPathResolvers().createResolvers(resolverConfig);
      } else {
        // default setup if nothing was configured
        String manifestCp = jfc.jarFile.getManifest().getMainAttributes().getValue(ManifestResolver.ATTR_NAME);
        if (manifestCp != null) {
          File repoRoot = new File(System.getProperty("user.home") + "/.m2/repository");
          if (repoRoot.exists()) {
            LOG.debug("Resolving manifest attribute {} based on {}", ManifestResolver.ATTR_NAME, repoRoot);
            resolvers.add(new ClassPathResolvers.ManifestResolver(repoRoot));
          } else {
            LOG.warn("Ignoring manifest attribute {} because {} does not exist.", ManifestResolver.ATTR_NAME, repoRoot);
          }
        }
      }

      for (Resolver r : resolvers) {
        r.resolve(jfc);
      }

      jfc.jarFile.close();

      URLConnection urlConnection = mainJarUrl.openConnection();
      if (urlConnection instanceof JarURLConnection) {
        // JDK6 keeps jar file shared and open as long as the process is running.
        // we want the jar file to be opened on every launch to pick up latest changes
        // http://abondar-howto.blogspot.com/2010/06/howto-unload-jar-files-loaded-by.html
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4167874
        ((JarURLConnection)urlConnection).getJarFile().close();
      }
      clUrls = jfc.urls;
    } else {
      clUrls = new LinkedHashSet<>();
    }

    // add the jar dependencies
/*
    if (cp == null) {
      // dependencies from parent loader, if classpath can't be found from pom
      ClassLoader baseCl = StramAppLauncher.class.getClassLoader();
      if (baseCl instanceof URLClassLoader) {
        URL[] baseUrls = ((URLClassLoader)baseCl).getURLs();
        // launch class path takes precedence - add first
        clUrls.addAll(Arrays.asList(baseUrls));
      }
    }
*/

    String libjars = propertiesBuilder.conf.get(LIBJARS_CONF_KEY_NAME);
    if (libjars != null) {
      processLibJars(libjars, clUrls);
    }

    for (URL baseURL : clUrls) {
      LOG.debug("Dependency: {}", baseURL);
    }

    this.launchDependencies = clUrls;

    // we have the classpath dependencies, scan for java configurations
    findAppConfigClasses(classFileNames);

  }

  private void processLibJars(String libjars, Set<URL> clUrls) throws Exception
  {
    for (String libjar : libjars.split(",")) {
      // if hadoop file system, copy from hadoop file system to local
      URI uri = new URI(libjar);
      String scheme = uri.getScheme();
      if (scheme == null) {
        // expand wildcards
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setIncludes(new String[]{libjar});
        scanner.scan();
        String[] files = scanner.getIncludedFiles();
        for (String file : files) {
          clUrls.add(new URL("file:" + file));
        }
      } else if (scheme.equals("file")) {
        clUrls.add(new URL(libjar));
      } else {
        if (fs != null) {
          Path path = new Path(libjar);
          File dependencyJarsDir = new File(StramClientUtils.getUserDTDirectory(), "dependencyJars");
          dependencyJarsDir.mkdirs();
          File localJarFile = new File(dependencyJarsDir, path.getName());
          fs.copyToLocalFile(path, new Path(localJarFile.getAbsolutePath()));
          clUrls.add(new URL("file:" + localJarFile.getAbsolutePath()));
        } else {
          throw new NotImplementedException("Jar file needs to be from Hadoop File System also in order for the dependency jars to be in Hadoop File System");
        }
      }
    }
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
        final Class<?> clazz = cl.loadClass(className);
        if (!Modifier.isAbstract(clazz.getModifiers()) && StreamingApplication.class.isAssignableFrom(clazz)) {
          final AppFactory appConfig = new StreamingAppFactory(classFileName, clazz)
          {
            @Override
            public LogicalPlan createApp(LogicalPlanConfiguration planConfig)
            {
              // load class from current context class loader
              Class<? extends StreamingApplication> c = StramUtils.classForName(className, StreamingApplication.class);
              StreamingApplication app = StramUtils.newInstance(c);
              return super.createApp(app, planConfig);
            }
          };
          appResourceList.add(appConfig);
        }
      } catch (Throwable e) { // java.lang.NoClassDefFoundError
        LOG.error("Unable to load class: " + className + " " + e);
      }
    }
  }

  public static Configuration getOverriddenConfig(Configuration conf, String overrideConfFileName, Map<String, String> overrideProperties) throws IOException
  {
    if (overrideConfFileName != null) {
      File overrideConfFile = new File(overrideConfFileName);
      if (overrideConfFile.exists()) {
        LOG.info("Loading settings: " + overrideConfFile.toURI());
        conf.addResource(new Path(overrideConfFile.toURI()));
      } else {
        throw new IOException("Problem opening file " + overrideConfFile);
      }
    }
    if (overrideProperties != null) {
      for (Map.Entry<String, String> entry : overrideProperties.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    StramClientUtils.evalConfiguration(conf);
    return conf;
  }

  public LogicalPlanConfiguration getLogicalPlanConfiguration()
  {
    return propertiesBuilder;
  }

  /**
   * Run application in-process. Returns only once application completes.
   *
   * @param appConfig
   * @throws Exception
   */
  public void runLocal(AppFactory appConfig) throws Exception
  {
    propertiesBuilder.conf.setEnum(StreamingApplication.ENVIRONMENT, StreamingApplication.Environment.LOCAL);
    LogicalPlan lp = appConfig.createApp(propertiesBuilder);

    String libJarsCsv = lp.getAttributes().get(Context.DAGContext.LIBRARY_JARS);
    if (libJarsCsv != null && libJarsCsv.length() != 0) {
      String[] split = libJarsCsv.split(StramClient.LIB_JARS_SEP);
      for (String jarPath : split) {
        File file = new File(jarPath);
        URL url = file.toURI().toURL();
        launchDependencies.add(url);
      }
    }

    // local mode requires custom classes to be resolved through the context class loader
    loadDependencies();
    StramLocalCluster lc = new StramLocalCluster(lp);
    lc.run();
  }

  public URLClassLoader loadDependencies()
  {
    if (this.loaderThread == null && this.initialClassLoader == null) {
      this.loaderThread = Thread.currentThread();
      this.initialClassLoader = Thread.currentThread().getContextClassLoader();
    }

    if (Thread.currentThread() != this.loaderThread) {
      throw new RuntimeException("Calls to loadDependencies can only be made on the same thread that loadDependencies was called on for the first time");
    } else {
      URL[] dependencies = launchDependencies.toArray(new URL[launchDependencies.size()]);

      ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();
      URLClassLoader cl = URLClassLoader.newInstance(dependencies, currentContextClassLoader);
      Thread.currentThread().setContextClassLoader(cl);

      StringCodecs.check();
      return cl;
    }
  }

  public void resetContextClassLoader()
  {
    if (Thread.currentThread() != this.loaderThread) {
      throw new RuntimeException("Calls to resetContextClassLoader can only be made on the same thread that loadDependencies was called on for the first time");
    }

    Thread.currentThread().setContextClassLoader(initialClassLoader);
  }

  private void setTokenRefreshCredentials(LogicalPlan dag, Configuration conf) throws IOException
  {
    String principal = conf.get(StramClientUtils.TOKEN_REFRESH_PRINCIPAL, StramUserLogin.getPrincipal());
    String keytabPath = conf.get(StramClientUtils.TOKEN_REFRESH_KEYTAB, conf.get(StramClientUtils.KEY_TAB_FILE));
    if (keytabPath == null) {
      String keytab = StramUserLogin.getKeytab();
      if (keytab != null) {
        Path localKeyTabPath = new Path(keytab);
        try (FileSystem fs = StramClientUtils.newFileSystemInstance(conf)) {
          Path destPath = new Path(StramClientUtils.getApexDFSRootDir(fs, conf), localKeyTabPath.getName());
          if (!fs.exists(destPath)) {
            fs.copyFromLocalFile(false, false, localKeyTabPath, destPath);
          }
          keytabPath = destPath.toString();
        }
      }
    }
    LOG.debug("User principal is {}, keytab is {}", principal, keytabPath);
    if ((principal != null) && (keytabPath != null)) {
      dag.setAttribute(LogicalPlan.PRINCIPAL, principal);
      dag.setAttribute(LogicalPlan.KEY_TAB_FILE, keytabPath);
    } else {
      LOG.warn("Credentials for refreshing tokens not available, application may not be able to run indefinitely");
    }
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
    Configuration conf = propertiesBuilder.conf;
    conf.setEnum(StreamingApplication.ENVIRONMENT, StreamingApplication.Environment.CLUSTER);
    LogicalPlan dag = appConfig.createApp(propertiesBuilder);
    if (UserGroupInformation.isSecurityEnabled()) {
      long hdfsTokenMaxLifeTime = conf.getLong(StramClientUtils.DT_HDFS_TOKEN_MAX_LIFE_TIME, conf.getLong(StramClientUtils.HDFS_TOKEN_MAX_LIFE_TIME, StramClientUtils.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT));
      dag.setAttribute(LogicalPlan.HDFS_TOKEN_LIFE_TIME, hdfsTokenMaxLifeTime);
      LOG.debug("HDFS token life time {}", hdfsTokenMaxLifeTime);
      long hdfsTokenRenewInterval = conf.getLong(StramClientUtils.DT_HDFS_TOKEN_RENEW_INTERVAL, conf.getLong(StramClientUtils.HDFS_TOKEN_RENEW_INTERVAL, StramClientUtils.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT));
      dag.setAttribute(LogicalPlan.HDFS_TOKEN_RENEWAL_INTERVAL, hdfsTokenRenewInterval);
      LOG.debug("HDFS token renew interval {}", hdfsTokenRenewInterval);
      long rmTokenMaxLifeTime = conf.getLong(StramClientUtils.DT_RM_TOKEN_MAX_LIFE_TIME, conf.getLong(YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_KEY, YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT));
      dag.setAttribute(LogicalPlan.RM_TOKEN_LIFE_TIME, rmTokenMaxLifeTime);
      LOG.debug("RM token life time {}", rmTokenMaxLifeTime);
      long rmTokenRenewInterval = conf.getLong(StramClientUtils.DT_RM_TOKEN_RENEW_INTERVAL, conf.getLong(YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_KEY, YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT));
      dag.setAttribute(LogicalPlan.RM_TOKEN_RENEWAL_INTERVAL, rmTokenRenewInterval);
      LOG.debug("RM token renew interval {}", rmTokenRenewInterval);
      setTokenRefreshCredentials(dag, conf);
    }
    String tokenRefreshFactor = conf.get(StramClientUtils.TOKEN_ANTICIPATORY_REFRESH_FACTOR);
    if (tokenRefreshFactor != null && tokenRefreshFactor.trim().length() > 0) {
      double refreshFactor = Double.parseDouble(tokenRefreshFactor);
      dag.setAttribute(LogicalPlan.TOKEN_REFRESH_ANTICIPATORY_FACTOR, refreshFactor);
      LOG.debug("Token refresh anticipatory factor {}", refreshFactor);
    }
    StramClient client = new StramClient(conf, dag);
    try {
      client.start();
      LinkedHashSet<String> libjars = Sets.newLinkedHashSet();
      String libjarsCsv = conf.get(LIBJARS_CONF_KEY_NAME);
      if (libjarsCsv != null) {
        String[] jars = StringUtils.splitByWholeSeparator(libjarsCsv, StramClient.LIB_JARS_SEP);
        libjars.addAll(Arrays.asList(jars));
      }
      if (deployJars != null) {
        for (File deployJar : deployJars) {
          libjars.add(deployJar.getAbsolutePath());
        }
      }

      client.setResources(libjars);
      client.setFiles(conf.get(FILES_CONF_KEY_NAME));
      client.setArchives(conf.get(ARCHIVES_CONF_KEY_NAME));
      client.setOriginalAppId(conf.get(ORIGINAL_APP_ID));
      client.setQueueName(conf.get(QUEUE_NAME));
      String tags = conf.get(TAGS);
      if (tags != null) {
        for (String tag : tags.split(",")) {
          client.addTag(tag.trim());
        }
      }
      client.startApplication();
      return client.getApplicationReport().getApplicationId();
    } finally {
      client.stop();
    }
  }

  public List<AppFactory> getBundledTopologies()
  {
    return Collections.unmodifiableList(this.appResourceList);
  }

}
