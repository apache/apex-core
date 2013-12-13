/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.io.*;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class CLIProxy
{
  private static final Logger LOG = LoggerFactory.getLogger(CLIProxy.class);

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
        while ((line = br.readLine()) != null) {
          content.append(line);
        }
      }
      catch (IOException ex) {
        LOG.error("Caught exception", ex);
      }
    }

  }

  public static JSONObject getLogicalPlan(String jarUri, String appName) throws Exception
  {
    return issueCommand("show-logical-plan \"" + jarUri + "\" \"" + appName + "\"");
  }

  public static JSONObject launchApp(String jarUri, String appName, Map<String, String> properties) throws Exception
  {
    StringBuilder sb = new StringBuilder("launch ");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      sb.append("-D ");
      sb.append(entry.getKey());
      sb.append("=\"");
      sb.append(entry.getValue());
      sb.append("\" ");
    }
    sb.append(jarUri);
    sb.append(" \"");
    sb.append(appName);
    sb.append("\"");
    return issueCommand(sb.toString());
  }

  public static JSONObject getApplications(String jarUri) throws Exception
  {
    return issueCommand("show-logical-plan \"" + jarUri + "\"");
  }

  private static JSONObject issueCommand(String command) throws Exception
  {
    String shellCommand = "dtcli -r -e '" + command + "'";
    Process p = Runtime.getRuntime().exec(new String[] {"sh", "-c", shellCommand});
    StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream());
    StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream());
    errorGobbler.start();
    outputGobbler.start();
    int exitValue = p.waitFor();
    LOG.debug("Executed: {}", shellCommand);
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
