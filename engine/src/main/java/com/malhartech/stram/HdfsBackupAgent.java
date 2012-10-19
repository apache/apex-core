/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.OperatorSerDe;

public class HdfsBackupAgent implements BackupAgent
{
  private static final Logger logger = LoggerFactory.getLogger(HdfsBackupAgent.class);

  final String checkpointFsPath;
  final Configuration conf;
  HdfsBackupAgent(Configuration conf, String checkpointFsPath)
  {
    this.conf = conf;
    this.checkpointFsPath = checkpointFsPath;
  }

  @Override
  public void backup(String id, long windowId, Object o, OperatorSerDe serDe) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + "/" + id + "/" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    logger.debug("Backup path: {}", path);
    FSDataOutputStream output = fs.create(path);
    try {
      serDe.write(o, output);
    }
    finally {
      output.close();
    }

  }

  @Override
  public Object restore(String id, long windowId, OperatorSerDe serDe) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + "/" + id + "/" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    FSDataInputStream input = fs.open(path);
    try {
      return serDe.read(input);
    }
    finally {
      input.close();
    }
  }

  @Override
  public void delete(String id, long windowId) throws IOException {
    Path path = new Path(this.checkpointFsPath + "/" + id + "/" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    fs.delete(path, false);

  }

}