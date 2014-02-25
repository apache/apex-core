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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import com.datatorrent.api.StorageAgent;

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
  public void save(Object object, int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id + PATH_SEPARATOR + windowId);
    logger.debug("Saving: {}", path);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    FSDataOutputStream stream = fs.create(path);
    try {
      store(stream, object);
    }
    finally {
      stream.close();
    }
  }

  @Override
  public Object load(int operatorId, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + operatorId + PATH_SEPARATOR + windowId);
    logger.debug("Loading: {}", path);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    FSDataInputStream stream = fs.open(path);
    try {
      return retrieve(stream);
    }
    finally {
      stream.close();
    }
  }

  @Override
  public void delete(int id, long windowId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + id + PATH_SEPARATOR + windowId);
    logger.debug("Deleting: {}", path);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    fs.delete(path, false);
  }

  @Override
  public long[] getWindowIds(int operatorId) throws IOException
  {
    Path path = new Path(this.checkpointFsPath + PATH_SEPARATOR + operatorId);
    FileSystem fs = FileSystem.get(path.toUri(), conf);

    FileStatus[] files = fs.listStatus(path);
    if (files == null || files.length == 0) {
      throw new IOException("Storage Agent has not saved anything yet!");
    }

    long windowIds[] = new long[files.length];
    for (int i = files.length; i-- > 0;) {
      windowIds[i] = Long.parseLong(files[i].getPath().getName());
    }
    return windowIds;
  }

  @Override
  public String toString()
  {
    return checkpointFsPath;
  }

  public static void store(OutputStream stream, Object operator)
  {
    Output output = new Output(4096, Integer.MAX_VALUE);
    output.setOutputStream(stream);
    final Kryo k = new Kryo();
    k.writeClassAndObject(output, operator);
    output.flush();
  }

  public static Object retrieve(InputStream stream)
  {
    final Kryo k = new Kryo();
    k.setClassLoader(Thread.currentThread().getContextClassLoader());
    Input input = new Input(stream);
    return k.readClassAndObject(input);
  }

  private static final Logger logger = LoggerFactory.getLogger(FSStorageAgent.class);
}
