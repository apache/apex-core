/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.StreamGobbler;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * CLIProxy class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.2
 */
public class CLIProxy implements Closeable
{
  private static final Logger LOG = LoggerFactory.getLogger(CLIProxy.class);
  private String[] dtCliCommand = null;
  private Process process;
  private BufferedReader br;
  private final ExecutorService executor = Executors.newFixedThreadPool(1);
  private StreamGobbler errorGobbler;
  private final Map<String, String> env = new HashMap<String, String>();
  private long commandTimeoutMillis = 60000;
  private boolean debug = false;
  private static final String COMMAND_DELIMITER = "___COMMAND_DELIMITER___";

  @SuppressWarnings("serial")
  public static class CommandException extends Exception
  {
    public CommandException(String message)
    {
      super(message);
    }

  }

  public CLIProxy()
  {
  }

  public CLIProxy(String dtCliCommand, boolean debug)
  {
    this(new String[]{dtCliCommand}, debug);
  }

  public CLIProxy(String[] dtCliCommand, boolean debug)
  {
    this.dtCliCommand = dtCliCommand;
    this.debug = debug;
  }

  public void putenv(String key, String value)
  {
    env.put(key, value);
  }

  public void setCommandTimeoutMillis(long commandTimeoutMillis)
  {
    this.commandTimeoutMillis = commandTimeoutMillis;
  }

  public void start() throws IOException
  {
    List<String> parameters = new ArrayList<String>();
    if (dtCliCommand != null) {
      parameters.addAll(Arrays.asList(dtCliCommand));
    } else {
      parameters.add("dtcli");
    }
    parameters.add("-r");
    if (debug) {
      parameters.add("-vvvv");
    }
    if (StramUserLogin.getPrincipal() != null && StramUserLogin.getKeytab() != null) {
      parameters.add("-kp");
      parameters.add(StramUserLogin.getPrincipal());
      parameters.add("-kt");
      parameters.add(StramUserLogin.getKeytab());
    }
    parameters.add("-f");
    parameters.add(COMMAND_DELIMITER + "\n");
    ProcessBuilder pb = new ProcessBuilder(parameters);
    pb.environment().putAll(env);
    process = pb.start();
    errorGobbler = new StreamGobbler(process.getErrorStream());
    errorGobbler.start();
    br = new BufferedReader(new InputStreamReader(process.getInputStream()));
    consumePrompt(); // consume the first prompt
  }

  @Override
  public void close() throws IOException
  {
    if (br != null) {
      br.close();
    }
    if (process != null) {
      process.destroy();
    }
  }

  public JSONObject getLogicalPlan(String jarUri, String appName, List<String> libjars, boolean ignorePom) throws Exception
  {
    StringBuilder sb = new StringBuilder("show-logical-plan ");
    if (!libjars.isEmpty()) {
      sb.append("-libjars ");
      sb.append(StringUtils.join(libjars, ','));
      sb.append(" ");
    }
    if (ignorePom) {
      sb.append("-ignorepom ");
    }
    sb.append(jarUri);
    sb.append(" \"");
    sb.append(appName);
    sb.append("\"");
    return issueCommand(sb.toString());
  }

  public JSONObject launchApp(String jarUri, String appName, Map<String, String> properties, List<String> libjars, boolean ignorePom, String originalAppId) throws Exception
  {
    StringBuilder sb = new StringBuilder("launch -exactMatch ");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      sb.append("-D ");
      sb.append(entry.getKey());
      sb.append("=\"");
      sb.append(entry.getValue());
      sb.append("\" ");
    }
    if (!libjars.isEmpty()) {
      sb.append("-libjars \"");
      sb.append(StringUtils.join(libjars, ','));
      sb.append("\" ");
    }
    if (ignorePom) {
      sb.append("-ignorepom ");
    }
    if (!StringUtils.isBlank(originalAppId)) {
      sb.append("-originalAppId \"");
      sb.append(originalAppId);
      sb.append("\" ");
    }
    sb.append(jarUri);
    sb.append(" \"");
    sb.append(appName);
    sb.append("\"");
    return issueCommand(sb.toString());
  }

  public JSONObject getJarOperatorClasses(List<String> jarUrls, String parent) throws Exception
  {
    StringBuilder sb = new StringBuilder("get-jar-operator-classes \"");
    sb.append(StringUtils.join(jarUrls, ",")).append("\"");
    if (parent != null) {
      sb.append(" \"").append(parent).append("\"");
    }
    return issueCommand(sb.toString());
  }

  public JSONObject getJarOperatorProperties(List<String> jarUrls, String operatorClass) throws Exception
  {
    StringBuilder sb = new StringBuilder("get-jar-operator-properties \"");
    sb.append(StringUtils.join(jarUrls, ","));
    sb.append("\" \"").append(operatorClass).append("\"");
    return issueCommand(sb.toString());
  }

  public JSONObject getApplications(String jarUri) throws Exception
  {
    return issueCommand("show-logical-plan \"" + jarUri + "\"");
  }

  public JSONObject getAppPackageInfo(String file) throws Exception
  {
    return issueCommand("get-app-package-info \"" + file + "\"");
  }

  public JSONObject launchAppPackage(File appPackageLocalFile, String appName, String appPackageConfigName, String config, Map<String, String> overrideProperties, String originalAppId) throws Exception
  {
    StringBuilder sb = new StringBuilder("launch -exactMatch \"");
    sb.append(appPackageLocalFile.getAbsolutePath());
    sb.append("\" ");
    if (!StringUtils.isBlank(appPackageConfigName)) {
      sb.append("-apconf \"").append(appPackageConfigName).append("\" ");
    }
    if (!StringUtils.isBlank(config)) {
      sb.append("-conf \"").append(config).append("\" ");
    }
    if (!StringUtils.isBlank(originalAppId)) {
      sb.append("-originalAppId \"").append(originalAppId).append("\" ");
    }
    for (Map.Entry<String, String> property : overrideProperties.entrySet()) {
      sb.append("-D \"").append(property.getKey()).append("=").append(property.getValue()).append("\" ");
    }
    sb.append("\"").append(appName).append("\"");

    return issueCommand(sb.toString());
  }

  public JSONObject getAppPackageOperatorClasses(File appPackageLocalFile, String parent, String searchTerm) throws Exception
  {
    StringBuilder sb = new StringBuilder("get-app-package-operators \"");
    if (!StringUtils.isBlank(parent)) {
      sb.append("-parent \"").append(parent).append("\"");
      sb.append("\" ");
    }
    sb.append(appPackageLocalFile.getAbsolutePath());
    if (!StringUtils.isBlank(searchTerm)) {
      sb.append(" \"").append(StringEscapeUtils.escapeJava(searchTerm)).append("\"");
    }
    return issueCommand(sb.toString());
  }

  public JSONObject getAppPackageOperatorProperties(File appPackageLocalFile, String clazz) throws Exception
  {
    StringBuilder sb = new StringBuilder("get-app-package-operator-properties \"");
    sb.append(appPackageLocalFile.getAbsolutePath());
    sb.append("\" ");
    sb.append("\"").append(clazz).append("\"");
    return issueCommand(sb.toString());
  }

  public JSONObject issueCommand(String command) throws Exception
  {
    OutputStream os = process.getOutputStream();
    LOG.debug("Issuing command to CLI: {}", command);
    os.write((command + "\n").getBytes());
    os.flush();
    Callable<String> readTask = new Callable<String>()
    {

      @Override
      public String call() throws Exception
      {
        StringBuilder sb = new StringBuilder();
        String line;
        while (true) {
          line = br.readLine();
          if (line == null) {
            LOG.warn("Unexpected EOF encountered from CLI proxy.");
            return sb.toString();
          }
          LOG.debug("From CLI, received: {}", line);
          if (COMMAND_DELIMITER.equals(line)) {
            break;
          }
          sb.append(line).append("\n");
        }
        return sb.toString();
      }

    };
    Future<String> future = executor.submit(readTask);
    String result = future.get(commandTimeoutMillis, TimeUnit.MILLISECONDS);
    String err = errorGobbler.getContent();
    os.write("echo $?\n".getBytes());
    os.flush();
    String status = readTask.call().trim();
    if (!"0".equals(status)) {
      LOG.error("Command failed and returned this in stderr: {}", err);
      throw new CommandException(err);
    }
    return (result == null) ? null : new JSONObject(result);
  }

  public JSONObject issueCommandRaw(String command) throws Exception
  {
    OutputStream os = process.getOutputStream();
    LOG.debug("Issuing command to CLI: {}", command);
    os.write((command + "\n").getBytes());
    os.flush();
    Callable<String> readTask = new Callable<String>()
    {

      @Override
      public String call() throws Exception
      {
        StringBuilder sb = new StringBuilder();
        String line;
        while (true) {
          line = br.readLine();
          if (line == null) {
            LOG.warn("Unexpected EOF encountered from CLI proxy.");
            return sb.toString();
          }
          LOG.debug("From CLI, received: {}", line);
          if (COMMAND_DELIMITER.equals(line)) {
            break;
          }
          sb.append(line).append("\n");
        }
        return sb.toString();
      }

    };
    Future<String> future = executor.submit(readTask);
    String out = future.get(commandTimeoutMillis, TimeUnit.MILLISECONDS);
    String err = errorGobbler.getContent();
    os.write("echo $?\n".getBytes());
    os.flush();
    String status = readTask.call().trim();
    JSONObject result = new JSONObject();
    result.put("status", status);
    result.put("out", out);
    result.put("err", err);
    return result;
  }

  private void consumePrompt() throws IOException
  {
    String prompt = br.readLine(); // consume the next prompt
    if (prompt == null) {
      throw new EOFException("Unexpected EOF reached");
    }

    // hack to ignore JVM warning when loading hadoop native library in stdout and there is no way to disable this warning in JVM
    while (prompt.contains("VM warning: You have loaded library")
            || prompt.contains("It's highly recommended that you fix the library with 'execstack -c <libfile>'")) {
      prompt = br.readLine();
    }

    LOG.debug("From CLI, received (prompt): {}", prompt);
    if (!COMMAND_DELIMITER.equals(prompt)) {
      throw new IOException(String.format("CLIProxy: expected \"%s\" but got \"%s\"", COMMAND_DELIMITER, prompt));
    }
  }

}
