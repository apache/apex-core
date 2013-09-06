
/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 *
 * @since 0.3.2
 */
package com.datatorrent.stram;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StorageAgent;
public class HdfsStorageAgent implements StorageAgent
{
  private static final String PATH_SEPARATOR = "/";
  final String checkpointFsPath;
  final Configuration conf;

  HdfsStorageAgent(Configuration conf, String checkpointFsPath)
  {
    this.conf = conf;
    this.checkpointFsPath = checkpointFsPath;
  }

  @Override
  public OutputStream getSaveStream(int id, long windowId) throws IOException
  {
    //logger.debug("Saving: {}{}{}{}{}", checkpointFsPath, PATH_SEPARATOR, id, PATH_SEPARATOR, windowId);
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id + PATH_SEPARATOR + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.create(path);
  }

  @Override
  public InputStream getLoadStream(int id, long windowId) throws IOException
  {
    //logger.debug("Loading: {}{}{}{}{}", checkpointFsPath, PATH_SEPARATOR, id, PATH_SEPARATOR, windowId);
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id + PATH_SEPARATOR + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.open(path);
  }

  @Override
  public void delete(int id, long windowId) throws IOException
  {
    //logger.debug("Deleting: {}{}{}{}{}", checkpointFsPath, PATH_SEPARATOR, id, PATH_SEPARATOR, windowId);
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id + PATH_SEPARATOR + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    fs.delete(path, false);
  }

  @Override
  public Long getMostRecentWindowId(int id) throws IOException {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    Long mrWindowId = null;
    try {
      FileStatus[] files = fs.listStatus(path);
      for (FileStatus fst : files) {
        long windowId = Long.valueOf(fst.getPath().getName());
        mrWindowId = (mrWindowId == null || windowId > mrWindowId) ? windowId : mrWindowId;
      }
    } catch (FileNotFoundException e) {
      // ignore
    }
    return mrWindowId;
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(HdfsStorageAgent.class);
}
