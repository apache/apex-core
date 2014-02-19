
/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 *
 * @since 0.3.2
 */
package com.datatorrent.stram;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.StorageAgent;
import com.datatorrent.bufferserver.util.Codec;
public class FSStorageAgent implements StorageAgent
{
  private static final String PATH_SEPARATOR = "/";
  final String checkpointFsPath;
  final Configuration conf;

  FSStorageAgent(Configuration conf, String checkpointFsPath)
  {
    this.conf = conf;
    this.checkpointFsPath = checkpointFsPath;
  }

  public FSStorageAgent(String checkpointFsPath)
  {
    conf = new Configuration();
    this.checkpointFsPath = checkpointFsPath;
  }

  @Override
  public OutputStream getSaveStream(int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id + PATH_SEPARATOR + Codec.getStringWindowId(windowId));
    logger.debug("Saving: {}", path);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.create(path);
  }

  @Override
  public InputStream getLoadStream(int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id + PATH_SEPARATOR + Codec.getStringWindowId(windowId));
    logger.debug("Loading: {}", path);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.open(path);
  }

  @Override
  public void delete(int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id + PATH_SEPARATOR + Codec.getStringWindowId(windowId));
    logger.debug("Deleting: {}", path);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    fs.delete(path, false);
  }

  @Override
  public long getMostRecentWindowId(int id) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id);
    FileSystem fs = FileSystem.get(path.toUri(), conf);

    FileStatus[] files = fs.listStatus(path);
    if (files == null || files.length == 0) {
      throw new IOException("Storage Agent has not saved anything yet!");
    }

    long mrWindowId = Codec.getLongWindowId(files[files.length - 1].getPath().getName());
    for (int i = files.length - 1; i-- > 0;) {
      long windowId = Codec.getLongWindowId(files[i].getPath().getName());
      if (windowId > mrWindowId) {
        mrWindowId = windowId;
      }
    }

    return mrWindowId;
  }

  @Override
  public String toString()
  {
    return checkpointFsPath;
  }

  private static final Logger logger = LoggerFactory.getLogger(FSStorageAgent.class);
}
