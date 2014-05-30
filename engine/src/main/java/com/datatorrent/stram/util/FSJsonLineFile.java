/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.lib.util.JacksonObjectMapperProvider;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class FSJsonLineFile
{
  private final FileSystem fs;
  private final Path path;
  private final ObjectMapper objectMapper;
  private static final Logger LOG = LoggerFactory.getLogger(FSJsonLineFile.class);

  public FSJsonLineFile(Path path, FsPermission permission) throws IOException
  {
    fs = FileSystem.newInstance(path.toUri(), new Configuration());
    this.path = path;
    LOG.debug("Path: {}", path.toUri());
    if (!fs.exists(path)) {
      FSDataOutputStream os = FileSystem.create(fs, path, permission);
      os.close();
    }

    boolean flag = fs.getConf().getBoolean("dfs.support.append", true);
    if (!flag) {
      throw new IOException("DFS append is not supported!");
    }
    this.objectMapper = (new JacksonObjectMapperProvider()).getContext(null);
  }

  public void append(JSONObject json) throws IOException
  {
    PrintWriter writer = new PrintWriter(fs.append(path));
    try {
      writer.append(json.toString() + "\n");
      writer.flush();
    }
    finally {
      writer.close();
    }
  }

  public void append(Object obj) throws IOException
  {
    PrintWriter writer = new PrintWriter(fs.append(path));
    try {
      writer.append(objectMapper.writeValueAsString(obj) + "\n");
      writer.flush();
    }
    finally {
      writer.close();
    }
  }

}
