/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.lib.util.JacksonObjectMapperProvider;
import java.io.Closeable;
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
public class FSJsonLineFile implements Closeable
{
  private final FileSystem fs;
  private final ObjectMapper objectMapper;
  private final FSDataOutputStream os;
  private static final Logger LOG = LoggerFactory.getLogger(FSJsonLineFile.class);

  public FSJsonLineFile(Path path, FsPermission permission) throws IOException
  {
    fs = FileSystem.newInstance(path.toUri(), new Configuration());
    os = FileSystem.create(fs, path, permission);
    this.objectMapper = (new JacksonObjectMapperProvider()).getContext(null);
  }

  public void append(JSONObject json) throws IOException
  {
    os.writeBytes(json.toString() + "\n");
    os.hflush();
  }

  public void append(Object obj) throws IOException
  {
    os.writeBytes(objectMapper.writeValueAsString(obj) + "\n");
    os.hflush();
  }

  @Override
  public void close() throws IOException
  {
    fs.close();
  }

}
