
/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 *
 * @since 0.3.2
 */
package com.datatorrent.stram;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.common.util.DTThrowable;

/**
 * FSStorageAgent
 *
 * @since 2.0.0
 */
public class FSStorageAgent implements StorageAgent, Serializable
{
  public static final String TMP_FILE = "._COPYING_";
  protected static final String STATELESS_CHECKPOINT_WINDOW_ID = Long.toHexString(Stateless.WINDOW_ID);
  public final String path;
  protected final transient FileSystem fs;
  protected static final transient Kryo kryo;

  static {
    kryo = new Kryo();
  }

  @SuppressWarnings("unused")
  private FSStorageAgent()
  {
    path = null;
    fs = null;
  }

  public FSStorageAgent(String path, Configuration conf)
  {
    this.path = path;
    try {
      logger.debug("Initialize storage agent with {}.", path);
      Path lPath = new Path(path);
      fs = FileSystem.newInstance(lPath.toUri(), conf == null ? new Configuration() : conf);
      try {
        if (fs.mkdirs(lPath)) {
          fs.setWorkingDirectory(lPath);
        }
      }
      catch (IOException e) {
        // some file system (MapR) throw exception if folder exists
        if (!fs.exists(lPath)) {
          throw e;
        }
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

  @SuppressWarnings("ThrowFromFinallyBlock")
  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    String operatorIdStr = String.valueOf(operatorId);
    Path lPath = new Path(operatorIdStr, TMP_FILE);
    String window = Long.toHexString(windowId);
    boolean stateSaved = false;
    FSDataOutputStream stream = null;
    try {
      stream = fs.create(lPath);
      store(stream, object);
      stateSaved = true;
    }
    catch (Throwable t) {
      logger.debug("while saving {} {}", operatorId, window, t);
      stateSaved = false;
      DTThrowable.rethrow(t);
    }
    finally {
      try {
        if (stream != null) {
          stream.close();
        }
      }
      catch (IOException ie){
         stateSaved = false;
         throw new RuntimeException(ie);
      }
      finally {
        if (stateSaved) {
          logger.debug("Saving {}: {}", operatorId, window);
          fs.rename(lPath, new Path(operatorIdStr, window));
        }
      }
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
    for (int i = files.length; i-- > 0; ) {
      String name = files[i].getPath().getName();
      if (name.equals(TMP_FILE)) {
        continue;
      }
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
