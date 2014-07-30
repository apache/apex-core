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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>AppBundle class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 1.0.3
 */
public class AppBundle extends JarFile implements Closeable
{
  public static final String ATTRIBUTE_DT_ENGINE_VERSION = "DT-Engine-Version";
  public static final String ATTRIBUTE_DT_APP_BUNDLE_NAME = "DT-App-Bundle-Name";
  public static final String ATTRIBUTE_DT_APP_BUNDLE_VERSION = "DT-App-Bundle-Version";
  public static final String ATTRIBUTE_CLASS_PATH = "Class-Path";

  private final String appBundleName;
  private final String appBundleVersion;
  private final String dtEngineVersion;
  private final ArrayList<String> classPath = new ArrayList<String>();
  private final String directory;

  private final List<AppInfo> applications = new ArrayList<AppInfo>();
  private final List<String> appJars = new ArrayList<String>();
  private final List<String> appPropertiesFiles = new ArrayList<String>();

  private final Set<String> requiredProperties = new TreeSet<String>();
  private final Map<String, String> defaultProperties = new TreeMap<String, String>();
  private final Set<String> configs = new TreeSet<String>();

  public static class AppInfo
  {
    public final String name;
    public final String jarName;
    public final LogicalPlan dag;

    public AppInfo(String name, String jarName, LogicalPlan dag)
    {
      this.name = name;
      this.jarName = jarName;
      this.dag = dag;
    }

  }

  public AppBundle(File file) throws IOException, ZipException
  {
    this(file, false);
  }

  /**
   * Creates an App Bundle object.
   *
   * If app directory is to be processed, there may be resource leak in the class loader. Only pass true for short-lived applications
   *
   * @param file
   * @param processAppDirectory
   * @throws java.io.IOException
   * @throws net.lingala.zip4j.exception.ZipException
   */
  public AppBundle(File file, boolean processAppDirectory) throws IOException, ZipException
  {
    super(file);
    Manifest manifest = getManifest();
    if (manifest == null) {
      throw new IOException("Not a valid app bundle. MANIFEST.MF is not present.");
    }
    Attributes attr = manifest.getMainAttributes();
    appBundleName = attr.getValue(ATTRIBUTE_DT_APP_BUNDLE_NAME);
    appBundleVersion = attr.getValue(ATTRIBUTE_DT_APP_BUNDLE_VERSION);
    dtEngineVersion = attr.getValue(ATTRIBUTE_DT_ENGINE_VERSION);
    String classPathString = attr.getValue(ATTRIBUTE_CLASS_PATH);
    if (classPathString == null) {
      throw new IOException("Not a valid app bundle.  Class-Path is missing from MANIFEST.MF");
    }
    classPath.addAll(Arrays.asList(StringUtils.split(classPathString, " ")));

    ZipFile zipFile = new ZipFile(file);
    if (zipFile.isEncrypted()) {
      throw new ZipException("Encrypted app bundle not supported yet");
    }
    File newDirectory = new File("/tmp/dt-appBundle/" + System.currentTimeMillis());
    newDirectory.mkdirs();
    directory = newDirectory.getAbsolutePath();
    zipFile.extractAll(directory);
    if (processAppDirectory) {
      processAppDirectory(new File(newDirectory, "app"));
    }
    processConfDirectory(new File(newDirectory, "conf"));

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

  public String getAppBundleName()
  {
    return appBundleName;
  }

  public String getAppBundleVersion()
  {
    return appBundleVersion;
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
            applications.add(new AppInfo(appName, entry.getName(), stramAppLauncher.prepareDAG(appFactory)));
          }
        }
        catch (Exception ex) {
          LOG.error("Caught exception trying to process {}", entry.getName(), ex);
        }
      }
      else if (entry.getName().endsWith(".properties")) {
        // TBD
        appPropertiesFiles.add(entry.getName());
      }
      else {
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

  private static final Logger LOG = LoggerFactory.getLogger(AppBundle.class);

}
