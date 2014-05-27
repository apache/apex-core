/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 *
 * @since 0.3.2
 */
package com.datatorrent.stram;

import java.io.*;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.stram.util.FSUtil;

public class FSStorageAgent implements StorageAgent, Serializable
{
  private static final String STATELESS_CHECKPOINT_WINDOW_ID = Long.toHexString(Stateless.WINDOW_ID);
  public final String path;
  private final transient FileSystem fs;
  private static final transient Kryo kryo;

  static {
    kryo = new Kryo();
  }

  @SuppressWarnings("unused")
  private FSStorageAgent()
  {
    this(null, null);
  }

  public FSStorageAgent(String path, Configuration conf)
  {
    this.path = path;
    try {
      logger.debug("Initialize storage agent with {}.", path);
      Path lPath = new Path(path);
      fs = FileSystem.newInstance(lPath.toUri(), conf == null ? new Configuration() : conf);
      if (FSUtil.mkdirs(fs, lPath)) {
        fs.setWorkingDirectory(lPath);
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  @SuppressWarnings("FinalizeDeclaration")
  protected void finalize() throws Throwable
  {
    if (fs != null) {
      logger.debug("Finalize storage agent with {}.", path);
      fs.close();
    }
    super.finalize();
  }

  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    Path lPath = new Path(String.valueOf(operatorId), Long.toHexString(windowId));
    logger.debug("Saving: {}", lPath);

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

    fs.delete(lPath, false);
  }

  @Override
  public long[] getWindowIds(int operatorId) throws IOException
  {
    Path lPath = new Path(String.valueOf(operatorId));

    FileStatus[] files = fs.listStatus(lPath);
    if (files == null || files.length == 0) {
      throw new IOException("Storage Agent has not saved anything yet!");
    }

    long windowIds[] = new long[files.length];
    for (int i = files.length; i-- > 0;) {
      String name = files[i].getPath().getName();
      windowIds[i] = STATELESS_CHECKPOINT_WINDOW_ID.equals(name) ? Stateless.WINDOW_ID : Long.parseLong(name, 16);
    }
    return windowIds;
  }

  @Override
  public String toString()
  {
    return fs.toString();
  }

  public static void store(OutputStream stream, Object operator)
  {
    synchronized (kryo) {
      Output output = new Output(4096, Integer.MAX_VALUE);
      output.setOutputStream(stream);
      kryo.writeClassAndObject(output, operator);
      output.flush();
    }
  }

  public static Object retrieve(InputStream stream)
  {
    synchronized (kryo) {
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
      Input input = new Input(stream);
      return kryo.readClassAndObject(input);
    }
  }

  public Object readResolve() throws ObjectStreamException
  {
    return new FSStorageAgent(this.path, null);
  }

  private static final long serialVersionUID = 201404031201L;
  private static final Logger logger = LoggerFactory.getLogger(FSStorageAgent.class);
}
