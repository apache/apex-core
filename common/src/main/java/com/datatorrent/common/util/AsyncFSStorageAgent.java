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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamException;
import java.nio.file.Files;
import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.AsyncStorageAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Throwables;

/**
 * <p>AsyncFSStorageAgent class.</p>
 *
 * @since 3.1.0
 */
public class AsyncFSStorageAgent extends FSStorageAgent implements AsyncStorageAgent
{
  private final transient Configuration conf;
  private transient volatile String localBasePath;

  private boolean syncCheckpoint = false;

  @SuppressWarnings("unused")
  private AsyncFSStorageAgent()
  {
    super();
    conf = null;
  }

  public AsyncFSStorageAgent(String path, Configuration conf)
  {
    super(path, conf);
    this.conf = conf == null ? new Configuration() : conf;
  }

  /*
   * Storage Agent should internally manage localBasePath. It should not take it from user
   */
  @Deprecated
  public AsyncFSStorageAgent(String localBasePath, String path, Configuration conf)
  {
    this(path, conf);
    this.localBasePath = localBasePath;
  }

  @Override
  public void save(final Object object, final int operatorId, final long windowId) throws IOException
  {
    if (syncCheckpoint) {
      super.save(object, operatorId, windowId);
      return;
    }

    if (localBasePath == null) {
      synchronized (this) {
        if (localBasePath == null) {
          localBasePath = Files.createTempDirectory("chkp").toString();
          logger.info("using {} as the basepath for checkpointing.", localBasePath);
        }
      }
    }
    String operatorIdStr = String.valueOf(operatorId);
    File directory = new File(localBasePath, operatorIdStr);
    if (!directory.exists()) {
      directory.mkdirs();
    }
    try (FileOutputStream stream = new FileOutputStream(new File(directory, String.valueOf(windowId)))) {
      store(stream, object);
    }
  }

  public void copyToHDFS(final int operatorId, final long windowId) throws IOException
  {
    if (this.localBasePath == null) {
      throw new AssertionError("save() was not called before copyToHDFS");
    }
    String operatorIdStr = String.valueOf(operatorId);
    File directory = new File(localBasePath, operatorIdStr);
    String window = Long.toHexString(windowId);
    Path lPath = new Path(path + Path.SEPARATOR + operatorIdStr + Path.SEPARATOR + TMP_FILE);
    File srcFile = new File(directory, String.valueOf(windowId));
    FSDataOutputStream stream = null;
    boolean stateSaved = false;
    try {
      // Create the temporary file with OverWrite option to avoid dangling lease issue and avoid exception if file already exists
      stream = fileContext.create(lPath, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts.CreateParent.createParent());
      InputStream in = null;
      try {
        in = new FileInputStream(srcFile);
        IOUtils.copyBytes(in, stream, conf, false);
      } finally {
        IOUtils.closeStream(in);
      }
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
          fileContext.rename(lPath, new Path(path + Path.SEPARATOR + operatorIdStr + Path.SEPARATOR + window), Options.Rename.OVERWRITE);
        }
        FileUtil.fullyDelete(srcFile);
      }
    }
  }

  @Override
  public void flush(int operatorId, long windowId) throws IOException
  {
    // Checkpoint already present in HDFS during save, when syncCheckpoint is true.
    if (isSyncCheckpoint()) {
      return;
    }
    copyToHDFS(operatorId, windowId);
  }

  @Override
  public Object readResolve() throws ObjectStreamException
  {
    AsyncFSStorageAgent asyncFSStorageAgent = new AsyncFSStorageAgent(this.path, null);
    asyncFSStorageAgent.setSyncCheckpoint(syncCheckpoint);
    return asyncFSStorageAgent;
  }

  @Override
  public boolean isSyncCheckpoint()
  {
    return syncCheckpoint;
  }

  public void setSyncCheckpoint(boolean syncCheckpoint)
  {
    this.syncCheckpoint = syncCheckpoint;
  }

  private static final long serialVersionUID = 201507241610L;
  private static final Logger logger = LoggerFactory.getLogger(AsyncFSStorageAgent.class);
}
