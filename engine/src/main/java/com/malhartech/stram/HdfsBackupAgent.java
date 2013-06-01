/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.StorageAgent;

public class HdfsBackupAgent implements StorageAgent
{
  final String checkpointFsPath;
  final Configuration conf;

  HdfsBackupAgent(Configuration conf, String checkpointFsPath)
  {
    this.conf = conf;
    this.checkpointFsPath = checkpointFsPath;
  }

  @Override
  public OutputStream getSaveStream(int id, long windowId) throws IOException
  {
    logger.debug("Writing: {}/{}/{}", checkpointFsPath, id, windowId);
    Path path = new Path(this.checkpointFsPath + "-" + id + "-" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.create(path);
  }

  @Override
  public InputStream getLoadStream(int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + "-" + id + "-" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.open(path);
  }

  @Override
  public void delete(int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + "-" + id + "-" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    fs.delete(path, false);
  }

  private static final Logger logger = LoggerFactory.getLogger(HdfsBackupAgent.class);
}