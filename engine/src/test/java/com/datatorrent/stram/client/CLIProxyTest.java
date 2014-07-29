/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.client;

import org.junit.*;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class CLIProxyTest
{
  @Ignore
  @Test
  public void testCliProxy() throws Exception
  {
    String dtcliCommand = System.getProperty("user.dir") + "/src/main/scripts/dtcli";
    System.out.println("dtcli command is " + dtcliCommand);
    CLIProxy cp = new CLIProxy(dtcliCommand);
    cp.start();
    System.out.println(cp.issueCommand("list-apps"));

    try {
      System.out.println(cp.issueCommand("bad-command"));
      Assert.assertFalse("Bad command should throw an exception", true);
    }
    catch (Throwable t) {
      // good
      System.out.println("Exception thrown as expected");
    }

    System.out.println(cp.issueCommand("show-license-status"));
    cp.close();
  }

}
