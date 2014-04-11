/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Abstract FSPartFileAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.0
 */
public abstract class FSPartFileAgent extends StramAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(FSPartFileAgent.class);
  private final Map<String, String> lastIndexLines = new HashMap<String, String>();

  protected abstract IndexLine parseIndexLine(String line) throws JSONException;

  public FSPartFileAgent(FileSystem fs, Configuration conf)
  {
    super(fs, conf);
  }

  public void setLastIndexLine(String basePath, String line)
  {
    lastIndexLines.put(basePath, line);
  }

  protected static class IndexLine
  {
    public boolean isEndLine = false;
    public String partFile;
  }

  protected class IndexFileBufferedReader extends BufferedReader
  {
    private final String basePath;
    private boolean lastLineReturned = false;
    private String lastPartFile;

    IndexFileBufferedReader(InputStreamReader reader, String basePath)
    {
      super(reader);
      this.basePath = basePath;
    }

    public IndexLine readIndexLine() throws IOException, JSONException
    {
      String line = super.readLine();
      if (line == null && !lastLineReturned) {
        line = lastIndexLines.get(basePath);
        if (line == null) {
          return null;
        }
        lastLineReturned = true;
        IndexLine il = parseIndexLine(line);
        if (il.partFile != null && !il.partFile.equals(lastPartFile)) {
          return il;
        }
        else {
          lastIndexLines.remove(basePath);
        }
        return null;
      }
      else if (line != null) {
        IndexLine il = parseIndexLine(line);
        lastPartFile = il.partFile;
        return il;
      }
      return null;
    }

  }

}
