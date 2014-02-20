
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
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.StorageAgent;
import com.datatorrent.bufferserver.util.Codec;
import com.google.common.collect.Lists;
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
  public Collection<Long> getWindowsIds(int operatorId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + operatorId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);

    FileStatus[] files = fs.listStatus(path);
    if (files == null || files.length == 0) {
      throw new IOException("Storage Agent has not saved anything yet!");
    }

    List<Long> windowIds = Lists.newArrayListWithExpectedSize(files.length);
    for (FileStatus file : files) {
      windowIds.add(Codec.getLongWindowId(file.getPath().getName()));
    }
    return windowIds;
  }

  @Override
  public String toString()
  {
    return checkpointFsPath;
  }

  private static final Logger logger = LoggerFactory.getLogger(FSStorageAgent.class);
}
