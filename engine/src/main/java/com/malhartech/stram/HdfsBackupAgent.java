/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.BackupAgent;
import com.malhartech.api.OperatorCodec;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsBackupAgent implements BackupAgent
{
  final String checkpointFsPath;
  final Configuration conf;
  private final OperatorCodec serde;

  HdfsBackupAgent(Configuration conf, String checkpointFsPath, OperatorCodec serDe)
  {
    this.conf = conf;
    this.checkpointFsPath = checkpointFsPath;
    serde = serDe;
  }

  @Override
  public void backup(int id, long windowId, Object o) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + "/" + id + "/" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    logger.debug("Backup path: {}", path);
    FSDataOutputStream output = fs.create(path);
    try {
      serde.write(o, output);
    }
    finally {
      output.close();
    }

  }

  @Override
  public Object restore(int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + "/" + id + "/" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    FSDataInputStream input = fs.open(path);
    try {
      return serde.read(input);
    }
    finally {
      input.close();
    }
  }

  @Override
  public void delete(int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + "/" + id + "/" + windowId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    fs.delete(path, false);
  }

  private static final Logger logger = LoggerFactory.getLogger(HdfsBackupAgent.class);

  @Override
  public OperatorCodec getOperatorSerDe()
  {
    return serde;
  }
}