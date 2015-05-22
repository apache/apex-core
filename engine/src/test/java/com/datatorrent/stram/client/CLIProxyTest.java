/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.client;


import com.datatorrent.stram.cli.DTCli;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class CLIProxyTest
{
  @Test
  public void testCliProxy() throws Exception
  {
    String[] dtcliCommand = new String[]{"java", "-cp", System.getProperty("java.class.path"), DTCli.class.getCanonicalName()};
    logger.debug("dtcli command is {}", StringUtils.join(dtcliCommand, " "));
    CLIProxy cp = new CLIProxy(dtcliCommand, false);
    cp.start();
    JSONObject result = cp.issueCommand("echo \"{\\\"a\\\": 123}\"");
    Assert.assertEquals(123, result.getInt("a"));
    try {
      logger.debug("Bad Command: {}", cp.issueCommand("bad-command"));
      Assert.assertFalse("Bad command should throw an exception", true);
    }
    catch (Throwable t) {
      // good
      logger.debug("Exception thrown as expected");
    }

    cp.close();
  }

  private static final Logger logger = LoggerFactory.getLogger(CLIProxyTest.class);
}
