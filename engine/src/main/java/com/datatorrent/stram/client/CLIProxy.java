/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.util.StreamGobbler;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.lang.StringUtils;
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
  private String dtCliCommand = null;
  private Process process;
  private BufferedReader br;
  private final ExecutorService executor = Executors.newFixedThreadPool(1);
  private StreamGobbler errorGobbler;
  private final Map<String, String> env = new HashMap<String, String>();
  private static final long TIMEOUT_MILLIS = 10000;
  private static final String COMMAND_DELIMITER = "___COMMAND_DELIMITER___";

  @SuppressWarnings("serial")
  public static class CommandException extends Exception
  {
    public CommandException(String message)
    {
      super(message);
    }

  }

  public CLIProxy(String dtCliCommand)
  {
    this.dtCliCommand = dtCliCommand;
  }

  public void putenv(String key, String value)
  {
    env.put(key, value);
  }

  public void start() throws IOException
  {
    ProcessBuilder pb = new ProcessBuilder(dtCliCommand != null ? dtCliCommand : "dtcli", "-r", "-f", COMMAND_DELIMITER + "\n");
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

  public JSONObject launchApp(String jarUri, String appName, Map<String, String> properties, List<String> libjars, boolean ignorePom) throws Exception
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

  public JSONObject getAppBundleInfo(String file) throws Exception
  {
    return issueCommand("get-app-bundle-info \"" + file + "\"");
  }

  public JSONObject launchAppBundle(File appBundleLocalFile, String appName, String configName, Map<String, String> overrideProperties) throws Exception
  {
    StringBuilder sb = new StringBuilder("launch-app-bundle -exactMatch \"");
    sb.append(appBundleLocalFile.getAbsolutePath());
    sb.append("\" ");
    if (!StringUtils.isBlank(configName)) {
      sb.append(" -conf \"").append(configName).append("\"");
    }
    for (Map.Entry<String, String> property : overrideProperties.entrySet()) {
      sb.append("-D \"").append(property.getKey()).append("=").append(property.getValue()).append("\" ");
    }
    sb.append("\"").append(appName).append("\"");

    return issueCommand(sb.toString());
  }

  public JSONObject getAppBundleOperatorClasses(File appBundleLocalFile, String parent) throws Exception
  {
    StringBuilder sb = new StringBuilder("get-app-bundle-operators \"");
    sb.append(appBundleLocalFile.getAbsolutePath());
    sb.append("\" ");
    if (!StringUtils.isBlank(parent)) {
      sb.append("\"").append(parent).append("\"");
    }
    return issueCommand(sb.toString());
  }

  public JSONObject getAppBundleOperatorProperties(File appBundleLocalFile, String clazz) throws Exception
  {
    StringBuilder sb = new StringBuilder("get-app-bundle-operator-properties \"");
    sb.append(appBundleLocalFile.getAbsolutePath());
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
    String result = future.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    String err = errorGobbler.getContent();
    if (!err.isEmpty()) {
      throw new CommandException(err);
    }
    return (result == null) ? null : new JSONObject(result);
  }

  private void consumePrompt() throws IOException
  {
    String prompt = br.readLine(); // consume the next prompt
    LOG.debug("From CLI, received (prompt): {}", prompt);
    if (!COMMAND_DELIMITER.equals(prompt)) {
      throw new RuntimeException(String.format("CLIProxy: expected \"%s\" but got \"%s\"", COMMAND_DELIMITER, prompt));
    }
  }

}
