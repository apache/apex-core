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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;


/**
 * <p>
 * AppPackage class.</p>
 *
 * @since 1.0.3
 */
public class AppPackage implements Closeable
{
  public static final String ATTRIBUTE_DT_ENGINE_VERSION = "DT-Engine-Version";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_NAME = "DT-App-Package-Name";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_VERSION = "DT-App-Package-Version";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_GROUP_ID = "DT-App-Package-Group-Id";
  public static final String ATTRIBUTE_CLASS_PATH = "Class-Path";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_DISPLAY_NAME = "DT-App-Package-Display-Name";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_DESCRIPTION = "DT-App-Package-Description";

  private final String appPackageName;
  private final String appPackageVersion;
  private final String appPackageGroupId;
  private final String dtEngineVersion;
  private final String appPackageDescription;
  private final String appPackageDisplayName;
  private final ArrayList<String> classPath = new ArrayList<>();
  private final File directory;

  private final List<AppInfo> applications = new ArrayList<>();
  private final List<String> appJars = new ArrayList<>();
  private final List<String> appJsonFiles = new ArrayList<>();
  private final List<String> appPropertiesFiles = new ArrayList<>();

  private final Set<String> requiredProperties = new TreeSet<>();
  private final Map<String, PropertyInfo> defaultProperties = new TreeMap<>();
  private final Set<String> configs = new TreeSet<>();

  private final File resourcesDirectory;
  private final boolean cleanOnClose;

  public static class AppInfo
  {
    public final String name;
    public final String file;
    public final String type;
    public String displayName;
    public LogicalPlan dag;
    public String error;
    public String errorStackTrace;

    public Set<String> requiredProperties = new TreeSet<>();
    public Map<String, PropertyInfo> defaultProperties = new TreeMap<>();

    public AppInfo(String name, String file, String type)
    {
      this.name = name;
      this.file = file;
      this.type = type;
    }

  }

  public static class PropertyInfo
  {
    private final String value;
    private final String description;

    public PropertyInfo(final String value, final String description)
    {
      this.value = value;
      this.description = description;
    }

    public String getValue()
    {
      return value;
    }

    public String getDescription()
    {
      return description;
    }
  }

  public static class PropertyInfoSerializer extends JsonSerializer<PropertyInfo>
  {
    private boolean provideDescription;

    public PropertyInfoSerializer(boolean provideDescription)
    {
      this.provideDescription = provideDescription;
    }

    @Override
    public void serialize(
        PropertyInfo propertyInfo, JsonGenerator jgen, SerializerProvider provider)
      throws IOException, JsonProcessingException
    {
      if (provideDescription) {
        jgen.writeStartObject();
        jgen.writeStringField("value", propertyInfo.value);
        jgen.writeStringField("description", propertyInfo.description);
        jgen.writeEndObject();
      } else {
        jgen.writeString(propertyInfo.value);
      }
    }
  }

  public AppPackage(File file) throws IOException
  {
    this(new FileInputStream(file));
  }

  public AppPackage(InputStream input) throws IOException
  {
    this(input, false);
  }

  /**
   * Creates an App Package object.
   *
   * If app directory is to be processed, there may be resource leak in the class loader. Only pass true for short-lived
   * applications
   *
   * If contentFolder is not null, it will try to create the contentFolder, file will be retained on disk after App Package is closed
   * If contentFolder is null, temp folder will be created and will be cleaned on close()
   *
   * @param file
   * @param contentFolder  the folder that the app package will be extracted to
   * @param processAppDirectory
   * @throws java.io.IOException
   */
  public AppPackage(File file, File contentFolder, boolean processAppDirectory) throws IOException
  {
    this(new FileInputStream(file), contentFolder, processAppDirectory);
  }

  /**
   * Creates an App Package object.
   *
   * If app directory is to be processed, there may be resource leak in the class loader. Only pass true for short-lived
   * applications
   *
   * If contentFolder is not null, it will try to create the contentFolder, file will be retained on disk after App Package is closed
   * If contentFolder is null, temp folder will be created and will be cleaned on close()
   *
   * @param input
   * @param contentFolder  the folder that the app package will be extracted to
   * @param processAppDirectory
   * @throws java.io.IOException
   */
  public AppPackage(InputStream input, File contentFolder, boolean processAppDirectory) throws IOException
  {
    try (final ZipArchiveInputStream zipArchiveInputStream =
        new ZipArchiveInputStream(input, "UTF8", true, true)) {

      if (contentFolder != null) {
        FileUtils.forceMkdir(contentFolder);
        cleanOnClose = false;
      } else {
        cleanOnClose = true;
        contentFolder = Files.createTempDirectory("dt-appPackage-").toFile();
      }
      directory = contentFolder;

      Manifest manifest = extractToDirectory(directory, zipArchiveInputStream);
      if (manifest == null) {
        throw new IOException("Not a valid app package. MANIFEST.MF is not present.");
      }
      Attributes attr = manifest.getMainAttributes();
      appPackageName = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_NAME);
      appPackageVersion = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_VERSION);
      appPackageGroupId = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_GROUP_ID);
      dtEngineVersion = attr.getValue(ATTRIBUTE_DT_ENGINE_VERSION);
      appPackageDisplayName = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_DISPLAY_NAME);
      appPackageDescription = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_DESCRIPTION);
      String classPathString = attr.getValue(ATTRIBUTE_CLASS_PATH);
      if (appPackageName == null || appPackageVersion == null || classPathString == null) {
        throw new IOException("Not a valid app package.  App Package Name or Version or Class-Path is missing from MANIFEST.MF");
      }
      classPath.addAll(Arrays.asList(StringUtils.split(classPathString, " ")));

      File confDirectory = new File(directory, "conf");
      if (confDirectory.exists()) {
        processConfDirectory(confDirectory);
      }
      resourcesDirectory = new File(directory, "resources");

      File propertiesXml = new File(directory, "META-INF/properties.xml");
      if (propertiesXml.exists()) {
        processPropertiesXml(propertiesXml, null);
      }

      if (processAppDirectory) {
        processAppDirectory(false);
      }
    }
  }

  private void processAppProperties()
  {
    for (AppInfo app : applications) {
      app.requiredProperties.addAll(requiredProperties);
      app.defaultProperties.putAll(defaultProperties);
      File appPropertiesXml = new File(directory, "META-INF/properties-" + app.name + ".xml");
      if (appPropertiesXml.exists()) {
        processPropertiesXml(appPropertiesXml, app);
      }
    }
  }

  /**
   * Creates an App Package object.
   *
   * If app directory is to be processed, there may be resource leak in the class loader. Only pass true for short-lived
   * applications
   *
   * Files in app package will be extracted to tmp folder and will be cleaned on close()
   * The close() method could be explicitly called or implicitly called by GC finalize()
   *
   * @param file
   * @param processAppDirectory
   * @throws java.io.IOException
   */
  public AppPackage(File file, boolean processAppDirectory) throws IOException
  {
    this(new FileInputStream(file), processAppDirectory);
  }

  /**
   * Creates an App Package object.
   *
   * If app directory is to be processed, there may be resource leak in the class loader. Only pass true for short-lived
   * applications
   *
   * Files in app package will be extracted to tmp folder and will be cleaned on close()
   * The close() method could be explicitly called or implicitly called by GC finalize()
   *
   * @param input
   * @param processAppDirectory
   * @throws java.io.IOException
   */
  public AppPackage(InputStream input, boolean processAppDirectory) throws IOException
  {
    this(input, null, processAppDirectory);
  }

  public static void extractToDirectory(File directory, File appPackageFile) throws IOException
  {
    extractToDirectory(directory, new ZipArchiveInputStream(new FileInputStream(appPackageFile), "UTF-8", true, true));
  }

  private static Manifest extractToDirectory(File directory, ZipArchiveInputStream input) throws IOException
  {
    Manifest manifest = null;
    File manifestFile = new File(directory, JarFile.MANIFEST_NAME);
    manifestFile.getParentFile().mkdirs();

    ZipArchiveEntry entry = input.getNextZipEntry();
    while (entry != null) {
      File newFile = new File(directory, entry.getName());
      if (entry.isDirectory()) {
        newFile.mkdirs();
      } else {
        if (JarFile.MANIFEST_NAME.equals(entry.getName())) {
          manifest = new Manifest(input);
          try (FileOutputStream output = new FileOutputStream(newFile)) {
            manifest.write(output);
          }
        } else {
          try (FileOutputStream output = new FileOutputStream(newFile)) {
            IOUtils.copy(input, output);
          }
        }
      }
      entry = input.getNextZipEntry();
    }
    return manifest;
  }

  public static void createAppPackageFile(File fileToBeCreated, File directory) throws ZipException
  {
    ZipFile zipFile = new ZipFile(fileToBeCreated);
    ZipParameters params = new ZipParameters();
    params.setIncludeRootFolder(false);
    zipFile.addFolder(directory, params);
  }

  public File tempDirectory()
  {
    return directory;
  }

  @Override
  public void close() throws IOException
  {
    if (cleanOnClose) {
      cleanContent();
    }
  }

  public void cleanContent() throws IOException
  {
    FileUtils.deleteDirectory(directory);
    LOG.debug("App Package {}-{} folder {} is removed", appPackageName, appPackageVersion, directory.getAbsolutePath());
  }

  public String getAppPackageName()
  {
    return appPackageName;
  }

  public String getAppPackageVersion()
  {
    return appPackageVersion;
  }

  public String getAppPackageGroupId()
  {
    return appPackageGroupId;
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

  public File resourcesDirectory()
  {
    return resourcesDirectory;
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

  public Map<String, PropertyInfo> getDefaultProperties()
  {
    return Collections.unmodifiableMap(defaultProperties);
  }

  public void processAppDirectory(boolean skipJars)
  {
    File dir = new File(directory, "app");
    applications.clear();

    Configuration config = new Configuration();

    for (Map.Entry<String, PropertyInfo> entry : defaultProperties.entrySet()) {
      config.set(entry.getKey(), entry.getValue().getValue());
    }

    List<String> absClassPath = new ArrayList<>(classPath);
    for (int i = 0; i < absClassPath.size(); i++) {
      String path = absClassPath.get(i);
      if (!path.startsWith("/")) {
        absClassPath.set(i, directory + "/" + path);
      }
    }
    config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, StringUtils.join(absClassPath, ','));
    File[] files = dir.listFiles();
    for (File entry : files) {

      if (entry.getName().endsWith(".jar") && !skipJars) {
        appJars.add(entry.getName());
        StramAppLauncher stramAppLauncher = null;
        try {
          stramAppLauncher = new StramAppLauncher(entry, config);
          stramAppLauncher.loadDependencies();
          List<AppFactory> appFactories = stramAppLauncher.getBundledTopologies();
          for (AppFactory appFactory : appFactories) {
            String appName = stramAppLauncher.getLogicalPlanConfiguration().getAppAlias(appFactory.getName());
            if (appName == null) {
              appName = appFactory.getName();
            }
            AppInfo appInfo = new AppInfo(appName, entry.getName(), "class");
            appInfo.displayName = appFactory.getDisplayName();
            try {
              appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
            } catch (Throwable ex) {
              appInfo.error = ex.getMessage();
              appInfo.errorStackTrace = ExceptionUtils.getStackTrace(ex);
            }
            applications.add(appInfo);
          }
        } catch (Exception ex) {
          LOG.error("Caught exception trying to process {}", entry.getName(), ex);
        } finally {
          if (stramAppLauncher != null) {
            stramAppLauncher.resetContextClassLoader();
          }
        }
      }
    }

    // this is for the properties and json files to be able to depend on the app jars,
    // since it's possible for users to implement the operators as part of the app package
    for (String appJar : appJars) {
      absClassPath.add(new File(dir, appJar).getAbsolutePath());
    }
    config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, StringUtils.join(absClassPath, ','));
    files = dir.listFiles();
    for (File entry : files) {
      if (entry.getName().endsWith(".json")) {
        appJsonFiles.add(entry.getName());
        AppInfo appInfo = StramClientUtils.jsonFileToAppInfo(entry, config);

        if (appInfo != null) {
          applications.add(appInfo);
        }

      } else if (entry.getName().endsWith(".properties")) {
        appPropertiesFiles.add(entry.getName());
        try {
          AppFactory appFactory = new StramAppLauncher.PropertyFileAppFactory(entry);
          StramAppLauncher stramAppLauncher = new StramAppLauncher(entry.getName(), config);
          stramAppLauncher.loadDependencies();
          AppInfo appInfo = new AppInfo(appFactory.getName(), entry.getName(), "properties");
          appInfo.displayName = appFactory.getDisplayName();
          try {
            appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
          } catch (Throwable t) {
            appInfo.error = t.getMessage();
            appInfo.errorStackTrace = ExceptionUtils.getStackTrace(t);
          }
          applications.add(appInfo);
        } catch (Exception ex) {
          LOG.error("Caught exceptions trying to process {}", entry.getName(), ex);
        }
      } else if (!entry.getName().endsWith(".jar")) {
        LOG.warn("Ignoring file {} with unknown extension in app directory", entry.getName());
      }
    }

    processAppProperties();
  }

  private void processConfDirectory(File dir)
  {
    File[] files = dir.listFiles();
    for (File entry : files) {
      if (entry.getName().endsWith(".xml")) {
        configs.add(entry.getName());
      }
    }
  }

  private void processPropertiesXml(File file, AppInfo app)
  {
    DTConfiguration config = new DTConfiguration();
    try {
      config.loadFile(file);
      for (Map.Entry<String, String> entry : config) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (value == null) {
          if (app == null) {
            requiredProperties.add(key);
          } else {
            app.requiredProperties.add(key);
          }
        } else {
        //Attribute are platform specific, ignoring description provided in properties file
          String description = key.contains(".attr.") ? null : config.getDescription(key);
          PropertyInfo propertyInfo = new PropertyInfo(value, description);
          if (app == null) {
            defaultProperties.put(key, propertyInfo);
          } else {
            app.requiredProperties.remove(key);
            app.defaultProperties.put(key, propertyInfo);
          }
        }
      }
    } catch (Exception ex) {
      LOG.warn("Ignoring META_INF/properties.xml because of error", ex);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AppPackage.class);

}
