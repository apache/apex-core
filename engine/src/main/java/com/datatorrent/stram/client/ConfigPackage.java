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
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

/**
 * <p>
 * ConfigPackage class.</p>
 *
 * @since 2.1.0
 */
public class ConfigPackage extends JarFile implements Closeable
{

  public static final String ATTRIBUTE_DT_CONF_PACKAGE_NAME = "DT-Conf-Package-Name";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_NAME = "DT-App-Package-Name";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_GROUP_ID = "DT-App-Package-Group-Id";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_MIN_VERSION = "DT-App-Package-Min-Version";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_MAX_VERSION = "DT-App-Package-Max-Version";
  public static final String ATTRIBUTE_DT_CONF_PACKAGE_DESCRIPTION = "DT-Conf-Package-Description";
  public static final String ATTRIBUTE_CLASS_PATH = "Class-Path";
  public static final String ATTRIBUTE_FILES = "Files";

  private final String configPackageName;
  private final String appPackageName;
  private final String appPackageGroupId;
  private final String appPackageMinVersion;
  private final String appPackageMaxVersion;
  private final String configPackageDescription;
  private final ArrayList<String> classPath = new ArrayList<>();
  private final ArrayList<String> files = new ArrayList<>();
  private final String directory;

  private final Map<String, String> properties = new TreeMap<>();
  private final Map<String, Map<String, String>> appProperties = new TreeMap<>();
  private final List<AppPackage.AppInfo> applications = new ArrayList<>();

  /**
   * Creates an Config Package object.
   *
   * @param file
   * @throws java.io.IOException
   * @throws net.lingala.zip4j.exception.ZipException
   */
  public ConfigPackage(File file) throws IOException, ZipException
  {
    super(file);
    Manifest manifest = getManifest();
    if (manifest == null) {
      throw new IOException("Not a valid config package. MANIFEST.MF is not present.");
    }
    Attributes attr = manifest.getMainAttributes();
    configPackageName = attr.getValue(ATTRIBUTE_DT_CONF_PACKAGE_NAME);
    appPackageName = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_NAME);
    appPackageGroupId = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_GROUP_ID);
    appPackageMinVersion = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_MIN_VERSION);
    appPackageMaxVersion = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_MAX_VERSION);
    configPackageDescription = attr.getValue(ATTRIBUTE_DT_CONF_PACKAGE_DESCRIPTION);
    String classPathString = attr.getValue(ATTRIBUTE_CLASS_PATH);
    String filesString = attr.getValue(ATTRIBUTE_FILES);
    if (configPackageName == null) {
      throw new IOException("Not a valid config package.  DT-Conf-Package-Name is missing from MANIFEST.MF");
    }
    if (!StringUtils.isBlank(classPathString)) {
      classPath.addAll(Arrays.asList(StringUtils.split(classPathString, " ")));
    }
    if (!StringUtils.isBlank(filesString)) {
      files.addAll(Arrays.asList(StringUtils.split(filesString, " ")));
    }

    ZipFile zipFile = new ZipFile(file);
    if (zipFile.isEncrypted()) {
      throw new ZipException("Encrypted conf package not supported yet");
    }
    File newDirectory = Files.createTempDirectory("dt-configPackage-").toFile();
    newDirectory.mkdirs();
    directory = newDirectory.getAbsolutePath();
    zipFile.extractAll(directory);
    processPropertiesXml();
    processAppDirectory(new File(directory, "app"));
  }

  public List<AppPackage.AppInfo> getApplications()
  {
    return Collections.unmodifiableList(applications);
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

  public String getConfigPackageName()
  {
    return configPackageName;
  }

  public String getAppPackageName()
  {
    return appPackageName;
  }

  public String getAppPackageGroupId()
  {
    return appPackageGroupId;
  }

  public String getAppPackageMinVersion()
  {
    return appPackageMinVersion;
  }

  public String getAppPackageMaxVersion()
  {
    return appPackageMaxVersion;
  }

  public String getConfigPackageDescription()
  {
    return configPackageDescription;
  }

  public List<String> getClassPath()
  {
    return Collections.unmodifiableList(classPath);
  }

  public List<String> getFiles()
  {
    return Collections.unmodifiableList(files);
  }

  public Map<String, String> getProperties(String appName)
  {
    if (appName == null || !appProperties.containsKey(appName)) {
      return properties;
    } else {
      return appProperties.get(appName);
    }
  }

  private void processAppDirectory(File dir)
  {
    if (!dir.exists()) {
      return;
    }

    Configuration config = new Configuration();

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
      if (entry.getName().endsWith(".json")) {

        AppPackage.AppInfo appInfo = StramClientUtils.jsonFileToAppInfo(entry, config);

        if (appInfo != null) {
          applications.add(appInfo);
        }
      }
    }
  }

  private void processPropertiesXml()
  {
    File dir = new File(directory, "META-INF");
    File p = new File(dir, "properties.xml");

    if (p.exists()) {
      parsePropertiesXml(p, properties);
    }
    for (File file : dir.listFiles()) {
      String name = file.getName();
      if (name.length() > 15 && name.startsWith("properties-") && name.endsWith(".xml")) {
        String appName = name.substring(11, name.length() - 4);
        Map<String, String> dp = new TreeMap<>(properties);
        parsePropertiesXml(file, dp);
        appProperties.put(appName, dp);
      }
    }
  }

  private static void parsePropertiesXml(File file, Map<String, String> properties)
  {
    DTConfiguration config = new DTConfiguration();
    try {
      config.loadFile(file);
      for (Map.Entry<String, String> entry : config) {
        String key = entry.getKey();
        String value = entry.getValue();
        properties.put(key, value);
      }
    } catch (Exception ex) {
      LOG.warn("Ignoring {} because of error", ex, file.getName());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ConfigPackage.class);

}
