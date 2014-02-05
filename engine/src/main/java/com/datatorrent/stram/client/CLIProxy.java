/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.io.*;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>CLIProxy class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.2
 */
public class CLIProxy
{
  private static final Logger LOG = LoggerFactory.getLogger(CLIProxy.class);
  private String dtHome = null;

  @SuppressWarnings("serial")
  public static class CommandException extends Exception
  {
    public CommandException(String message)
    {
      super(message);
    }

  }

  private static class StreamGobbler extends Thread
  {
    InputStream is;
    StringBuilder content = new StringBuilder();

    StreamGobbler(InputStream is)
    {
      this.is = is;
    }

    String getContent()
    {
      return content.toString();
    }

    @Override
    public void run()
    {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line;
        try {
          while ((line = br.readLine()) != null) {
            if (!line.contains(" DEBUG ")) {
              content.append(line);
              content.append("\n");
            }
          }
        }
        finally {
          br.close();
        }
      }
      catch (IOException ex) {
        LOG.error("Caught exception", ex);
      }
    }

  }

  public CLIProxy(String dtHome)
  {
    this.dtHome = dtHome;
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
    StringBuilder sb = new StringBuilder("launch ");
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

  public JSONObject getApplications(String jarUri) throws Exception
  {
    return issueCommand("show-logical-plan \"" + jarUri + "\"");
  }

  private JSONObject issueCommand(String command) throws Exception
  {
    // we can optimize this by launching dtcli only once so subsequent issueCommand() calls won't take so long, and use stdin to issue commands to cli,
    // but we need to make sure the stdout and stderr from dtcli are in sync with the commands.
    // that probably means the streamglobber needs to go.
    String dtCliCommand = (dtHome == null) ? "dtcli" : (dtHome + "/bin/dtcli");
    String shellCommand = dtCliCommand + " -r -e '" + command + "'";
    Process p = Runtime.getRuntime().exec(new String[] {"bash", "-c", shellCommand});
    StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream());
    StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream());
    errorGobbler.start();
    outputGobbler.start();
    int exitValue = p.waitFor();
    LOG.debug("Executed: {} ; exit code: {}", shellCommand, exitValue);
    LOG.debug("Output: {}", outputGobbler.getContent());
    LOG.debug("Error: {}", errorGobbler.getContent());
    if (exitValue == 0) {
      return new JSONObject(outputGobbler.getContent());
    }
    else {
      throw new CommandException(errorGobbler.getContent());
    }
  }

}
