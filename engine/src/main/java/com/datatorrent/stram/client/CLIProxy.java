/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    Process p = Runtime.getRuntime().exec(new String[] { "sh", "-c", shellCommand } );
    String error = IOUtils.toString(p.getErrorStream());
    String result = IOUtils.toString(p.getInputStream());
    p.waitFor();
    int exitValue = p.exitValue();
    LOG.debug("Executed: {}", shellCommand);
    LOG.debug("Output: {}", result);
    LOG.debug("Error: {}", error);
    if (exitValue == 0) {
      return new JSONObject(result);
    } else {
      throw new CommandException(error);
    }
  }
}
