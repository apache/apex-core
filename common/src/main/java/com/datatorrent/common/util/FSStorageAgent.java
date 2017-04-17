/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Throwables;

import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;

/**
 * FSStorageAgent
 *
 * @since 0.3.2
 */
public class FSStorageAgent implements StorageAgent, Serializable
{
  public static final String TMP_FILE = "_tmp";
  protected static final String STATELESS_CHECKPOINT_WINDOW_ID = Long.toHexString(Stateless.WINDOW_ID);
  public final String path;
  protected final transient FileContext fileContext;
  protected static final transient Kryo kryo;

  static {
    kryo = new Kryo();
  }

  protected FSStorageAgent()
  {
    path = null;
    fileContext = null;
  }

  public FSStorageAgent(String path, Configuration conf)
  {
    this.path = path;
    try {
      logger.debug("Initialize storage agent with {}.", path);
      Path lPath = new Path(path);
      URI pathUri = lPath.toUri();

      if (pathUri.getScheme() != null) {
        fileContext = FileContext.getFileContext(pathUri, conf == null ? new Configuration() : conf);
      } else {
        fileContext = FileContext.getFileContext(conf == null ? new Configuration() : conf);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    String operatorIdStr = String.valueOf(operatorId);
    Path lPath = new Path(path + Path.SEPARATOR + operatorIdStr + Path.SEPARATOR + TMP_FILE);
    String window = Long.toHexString(windowId);
    boolean stateSaved = false;
    FSDataOutputStream stream = null;
    try {
      stream = fileContext.create(lPath, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
        Options.CreateOpts.CreateParent.createParent());
      store(stream, object);
      stateSaved = true;
    } catch (Throwable t) {
      logger.debug("while saving {} {}", operatorId, window, t);
      stateSaved = false;
      throw Throwables.propagate(t);
    } finally {
      try {
        if (stream != null) {
          stream.close();
        }
      } catch (IOException ie) {
        stateSaved = false;
        throw new RuntimeException(ie);
      } finally {
        if (stateSaved) {
          logger.debug("Saving {}: {}", operatorId, window);
          fileContext.rename(lPath, new Path(path + Path.SEPARATOR + operatorIdStr + Path.SEPARATOR + window),
              Options.Rename.OVERWRITE);
        }
      }
    }
  }

  @Override
  public Object load(int operatorId, long windowId) throws IOException
  {
    Path lPath = new Path(path + Path.SEPARATOR + String.valueOf(operatorId) + Path.SEPARATOR + Long.toHexString(windowId));
    logger.debug("Loading: {}", lPath);

    FSDataInputStream stream = fileContext.open(lPath);
    try {
      return retrieve(stream);
    } finally {
      stream.close();
    }
  }

  @Override
  public void delete(int operatorId, long windowId) throws IOException
  {
    Path lPath = new Path(path + Path.SEPARATOR + String.valueOf(operatorId) + Path.SEPARATOR + Long.toHexString(windowId));
    logger.debug("Deleting: {}", lPath);

    fileContext.delete(lPath, false);
  }

  @Override
  public long[] getWindowIds(int operatorId) throws IOException
  {
    Path lPath = new Path(path + Path.SEPARATOR + String.valueOf(operatorId));
    try {
      FileStatus status = fileContext.getFileStatus(lPath);
      if (!status.isDirectory()) {
        throw new RuntimeException("Checkpoint location is not a directory");
      }
    } catch (FileNotFoundException ex) {
      // During initialization checkpoint directory may not exists.
      fileContext.mkdir(lPath, FsPermission.getDirDefault(), true);
    }

    RemoteIterator<FileStatus> fileStatusRemoteIterator = fileContext.listStatus(lPath);
    List<Long> lwindows = new ArrayList<>();
    while (fileStatusRemoteIterator.hasNext()) {
      FileStatus fileStatus = fileStatusRemoteIterator.next();
      String name = fileStatus.getPath().getName();
      if (name.equals(TMP_FILE)) {
        continue;
      }
      lwindows.add(STATELESS_CHECKPOINT_WINDOW_ID.equals(name) ? Stateless.WINDOW_ID : Long.parseLong(name, 16));
    }
    long[] windowIds = new long[lwindows.size()];
    for (int i = 0; i < windowIds.length; i++) {
      windowIds[i] = lwindows.get(i);
    }
    return windowIds;
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
