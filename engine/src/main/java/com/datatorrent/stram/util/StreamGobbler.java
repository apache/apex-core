/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>StreamGobbler class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.4
 */
public class StreamGobbler extends Thread
{
  InputStream is;
  StringBuilder content = new StringBuilder();
  private static final Logger LOG = LoggerFactory.getLogger(StreamGobbler.class);

  public StreamGobbler(InputStream is)
  {
    this.is = is;
  }

  public String getContent()
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
