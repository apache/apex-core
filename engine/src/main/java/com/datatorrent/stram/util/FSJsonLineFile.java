/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.lib.util.JacksonObjectMapperProvider;
import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>FSJsonLineFile class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 1.0.2
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
    FSDataOutputStream myos;
    if (fs.exists(path)) {
      try {
        // happens if not the first application attempt
        myos = fs.append(path);
      }
      catch (IOException ex) {
        LOG.warn("Caught exception (OK during unit test): {}", ex.getMessage());
        myos = FileSystem.create(fs, path, permission);
      }
    }
    else {
      myos = FileSystem.create(fs, path, permission);
    }
    os = myos;
    this.objectMapper = (new JacksonObjectMapperProvider()).getContext(null);
  }

  public synchronized void append(JSONObject json) throws IOException
  {
    os.writeBytes(json.toString() + "\n");
    os.hflush();
  }

  public synchronized void append(Object obj) throws IOException
  {
    os.writeBytes(objectMapper.writeValueAsString(obj) + "\n");
    os.hflush();
  }

  @Override
  public void close() throws IOException
  {
    os.close();
    fs.close();
  }

}
