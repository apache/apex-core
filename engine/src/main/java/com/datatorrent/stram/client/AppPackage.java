/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import java.io.*;
import java.util.*;
import java.util.jar.*;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * AppPackage class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 1.0.3
 */
public class AppPackage extends JarFile implements Closeable
{
  public static final String ATTRIBUTE_DT_ENGINE_VERSION = "DT-Engine-Version";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_NAME = "DT-App-Package-Name";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_VERSION = "DT-App-Package-Version";
  public static final String ATTRIBUTE_CLASS_PATH = "Class-Path";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_DISPLAY_NAME = "DT-App-Package-Display-Name";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_DESCRIPTION = "DT-App-Package-Description";

  private final String appPackageName;
  private final String appPackageVersion;
  private final String dtEngineVersion;
  private final String appPackageDescription;
  private final String appPackageDisplayName;
  private final ArrayList<String> classPath = new ArrayList<String>();
  private final String directory;

  private final List<AppInfo> applications = new ArrayList<AppInfo>();
  private final List<String> appJars = new ArrayList<String>();
  private final List<String> appJsonFiles = new ArrayList<String>();
  private final List<String> appPropertiesFiles = new ArrayList<String>();

  private final Set<String> requiredProperties = new TreeSet<String>();
  private final Map<String, String> defaultProperties = new TreeMap<String, String>();
  private final Set<String> configs = new TreeSet<String>();

  public static class AppInfo
  {
    public final String name;
    public final String file;
    public final String type;
    public String displayName;
    public LogicalPlan dag;
    public String error;

    public AppInfo(String name, String file, String type)
    {
      this.name = name;
      this.file = file;
      this.type = type;
    }

  }

  public AppPackage(File file) throws IOException, ZipException
  {
    this(file, false);
  }

  /**
   * Creates an App Package object.
   *
   * If app directory is to be processed, there may be resource leak in the class loader. Only pass true for short-lived applications
   *
   * @param file
   * @param processAppDirectory
   * @throws java.io.IOException
   * @throws net.lingala.zip4j.exception.ZipException
   */
  public AppPackage(File file, boolean processAppDirectory) throws IOException, ZipException
  {
    super(file);
    Manifest manifest = getManifest();
    if (manifest == null) {
      throw new IOException("Not a valid app package. MANIFEST.MF is not present.");
    }
    Attributes attr = manifest.getMainAttributes();
    appPackageName = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_NAME);
    appPackageVersion = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_VERSION);
    dtEngineVersion = attr.getValue(ATTRIBUTE_DT_ENGINE_VERSION);
    appPackageDisplayName = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_DISPLAY_NAME);
    appPackageDescription = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_DESCRIPTION);
    String classPathString = attr.getValue(ATTRIBUTE_CLASS_PATH);
    if (appPackageName == null || appPackageVersion == null || classPathString == null) {
      throw new IOException("Not a valid app package.  Class-Path is missing from MANIFEST.MF");
    }
    classPath.addAll(Arrays.asList(StringUtils.split(classPathString, " ")));

    ZipFile zipFile = new ZipFile(file);
    if (zipFile.isEncrypted()) {
      throw new ZipException("Encrypted app package not supported yet");
    }
    File newDirectory = new File("/tmp/dt-appPackage-" + System.currentTimeMillis());
    newDirectory.mkdirs();
    directory = newDirectory.getAbsolutePath();
    zipFile.extractAll(directory);
    if (processAppDirectory) {
      processAppDirectory(new File(newDirectory, "app"));
    }
    File confDirectory = new File(newDirectory, "conf");
    if (confDirectory.exists()) {
      processConfDirectory(confDirectory);
    }
    File propertiesXml = new File(newDirectory, "META-INF/properties.xml");
    if (propertiesXml.exists()) {
      processPropertiesXml(propertiesXml);
    }
  }

  public String tempDirectory()
  {
    return directory;
  }

  @Override
  public void close() throws IOException
  {
    super.close();
    FileUtils.deleteDirectory(new File(directory));
  }

  public String getAppPackageName()
  {
    return appPackageName;
  }

  public String getAppPackageVersion()
  {
    return appPackageVersion;
  }

  public String getAppPackageDescription()
  {
    return appPackageDescription;
  }

  public String getAppPackageDisplayName()
  {
    return appPackageDisplayName;
  }

  public String getDtEngineVersion()
  {
    return dtEngineVersion;
  }

  public List<String> getClassPath()
  {
    return Collections.unmodifiableList(classPath);
  }

  public Collection<String> getConfigs()
  {
    return Collections.unmodifiableCollection(configs);
  }

  public List<AppInfo> getApplications()
  {
    return Collections.unmodifiableList(applications);
  }

  public List<String> getAppJars()
  {
    return Collections.unmodifiableList(appJars);
  }

  public List<String> getAppJsonFiles()
  {
    return Collections.unmodifiableList(appJsonFiles);
  }

  public List<String> getAppPropertiesFiles()
  {
    return Collections.unmodifiableList(appPropertiesFiles);
  }

  public Set<String> getRequiredProperties()
  {
    return Collections.unmodifiableSet(requiredProperties);
  }

  public Map<String, String> getDefaultProperties()
  {
    return Collections.unmodifiableMap(defaultProperties);
  }

  private void processAppDirectory(File dir)
  {
    Iterator<File> it = FileUtils.iterateFiles(dir, null, false);

    Configuration config = new Configuration();

    List<String> absClassPath = new ArrayList<String>(classPath);
    for (int i = 0; i < absClassPath.size(); i++) {
      String path = absClassPath.get(i);
      if (!path.startsWith("/")) {
        absClassPath.set(i, directory + "/" + path);
      }
    }
    config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, StringUtils.join(absClassPath, ','));

    while (it.hasNext()) {
      File entry = it.next();
      if (entry.getName().endsWith(".jar")) {
        appJars.add(entry.getName());
        try {
          StramAppLauncher stramAppLauncher = new StramAppLauncher(entry, config);
          stramAppLauncher.loadDependencies();
          List<AppFactory> appFactories = stramAppLauncher.getBundledTopologies();
          for (AppFactory appFactory : appFactories) {
            String appName = stramAppLauncher.getLogicalPlanConfiguration().getAppAlias(appFactory.getName());
            if (appName == null) {
              appName = appFactory.getName();
            }
            AppInfo appInfo = new AppInfo(appName, entry.getName(), "class");
            appInfo.displayName = appFactory.getDisplayName();
            appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
            try {
              appInfo.dag.validate();
            }
            catch (Exception ex) {
              appInfo.error = ex.getMessage();
            }
            applications.add(appInfo);
          }
        }
        catch (Exception ex) {
          LOG.error("Caught exception trying to process {}", entry.getName(), ex);
        }
      }
    }
    it = FileUtils.iterateFiles(dir, null, false);

    // this is for the properties and json files to be able to depend on the app jars,
    // since it's possible for users to implement the operators as part of the app package
    for (String appJar : appJars) {
      absClassPath.add(new File(dir, appJar).getAbsolutePath());
    }
    config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, StringUtils.join(absClassPath, ','));
    while (it.hasNext()) {
      File entry = it.next();
      if (entry.getName().endsWith(".json")) {
        appJsonFiles.add(entry.getName());
        try {
          AppFactory appFactory = new StramAppLauncher.JsonFileAppFactory(entry);
          StramAppLauncher stramAppLauncher = new StramAppLauncher(entry.getName(), config);
          stramAppLauncher.loadDependencies();
          AppInfo appInfo = new AppInfo(appFactory.getName(), entry.getName(), "json");
          appInfo.displayName = appFactory.getDisplayName();
          try {
            appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
            appInfo.dag.validate();
          }
          catch (Exception ex) {
            appInfo.error = ex.getMessage() + ": " + ExceptionUtils.getStackTrace(ex);
          }
          applications.add(appInfo);
        }
        catch (Exception ex) {
          LOG.error("Caught exceptions trying to process {}", entry.getName(), ex);
        }
      }
      else if (entry.getName().endsWith(".properties")) {
        appPropertiesFiles.add(entry.getName());
        try {
          AppFactory appFactory = new StramAppLauncher.PropertyFileAppFactory(entry);
          StramAppLauncher stramAppLauncher = new StramAppLauncher(entry.getName(), config);
          stramAppLauncher.loadDependencies();
          AppInfo appInfo = new AppInfo(appFactory.getName(), entry.getName(), "properties");
          appInfo.displayName = appFactory.getDisplayName();
          appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
          try {
            appInfo.dag.validate();
          }
          catch (Throwable t) {
            appInfo.error = t.getMessage();
          }
          applications.add(appInfo);
        }
        catch (Exception ex) {
          LOG.error("Caught exceptions trying to process {}", entry.getName(), ex);
        }
      }
      else if (!entry.getName().endsWith(".jar")) {
        LOG.warn("Ignoring file {} with unknown extension in app directory", entry.getName());
      }
    }
  }

  private void processConfDirectory(File dir)
  {
    Iterator<File> it = FileUtils.iterateFiles(dir, null, false);

    while (it.hasNext()) {
      File entry = it.next();
      if (entry.getName().endsWith(".xml")) {
        configs.add(entry.getName());
      }
    }
  }

  private void processPropertiesXml(File file)
  {
    DTConfiguration config = new DTConfiguration();
    try {
      config.loadFile(file);
      for (Map.Entry<String, String> entry : config) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (value == null) {
          requiredProperties.add(key);
        }
        else {
          defaultProperties.put(key, value);
        }
      }
    }
    catch (Exception ex) {
      LOG.warn("Ignoring META_INF/properties.xml because of error", ex);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AppPackage.class);

}
