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
import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import com.datatorrent.api.StorageAgent;
import com.datatorrent.stram.api.Checkpoint;

public class FSStorageAgent implements StorageAgent, Serializable
{
  private static final String STATELESS_CHECKPOINT_WINDOW_ID = Long.toHexString(Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID);
  public final String path;
  private final Configuration conf;
  private transient FileSystem fs;

  private FSStorageAgent()
  {
    /* this is needed just for serialization with Kryo */
    conf = null;
    path = null;
  }

  public FSStorageAgent(Configuration conf, String path)
  {
    this.conf = conf;
    this.path = path;
  }

  public void initialize() throws IOException
  {
    if (fs == null) {
      Path lPath = new Path(path);
      fs = FileSystem.newInstance(lPath.toUri(), conf);
      if (FileSystem.mkdirs(fs, lPath, new FsPermission((short)00755))) {
        fs.setWorkingDirectory(lPath);
      }
    }
  }

  @Override
  @SuppressWarnings("FinalizeDeclaration")
  protected void finalize() throws Throwable
  {
    if (fs != null) {
      fs.close();
    }
    super.finalize();
  }

  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    Path lPath = new Path(String.valueOf(operatorId), Long.toHexString(windowId));
    logger.debug("Saving: {}", lPath);

    initialize();
    FSDataOutputStream stream = fs.create(lPath);
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
    Path lPath = new Path(String.valueOf(operatorId), Long.toHexString(windowId));
    logger.debug("Loading: {}", lPath);

    initialize();
    FSDataInputStream stream = fs.open(lPath);
    try {
      return retrieve(stream);
    }
    finally {
      stream.close();
    }
  }

  @Override
  public void delete(int operatorId, long windowId) throws IOException
  {
    Path lPath = new Path(String.valueOf(operatorId), Long.toHexString(windowId));
    logger.debug("Deleting: {}", lPath);

    initialize();
    fs.delete(lPath, false);
  }

  @Override
  public long[] getWindowIds(int operatorId) throws IOException
  {
    Path lPath = new Path(String.valueOf(operatorId));

    initialize();
    FileStatus[] files = fs.listStatus(lPath);
    if (files == null || files.length == 0) {
      throw new IOException("Storage Agent has not saved anything yet!");
    }

    long windowIds[] = new long[files.length];
    for (int i = files.length; i-- > 0;) {
      String name = files[i].getPath().getName();
      windowIds[i] = STATELESS_CHECKPOINT_WINDOW_ID.equals(name) ? Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID : Long.parseLong(name, 16);
    }
    return windowIds;
  }

  @Override
  public String toString()
  {
    try {
      initialize();
      return fs.toString();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
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

  private static final long serialVersionUID = 201404031201L;
  private static final Logger logger = LoggerFactory.getLogger(FSStorageAgent.class);
}
