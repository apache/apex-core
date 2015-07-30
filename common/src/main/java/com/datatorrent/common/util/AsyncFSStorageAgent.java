/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.common.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectStreamException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncFSStorageAgent extends FSStorageAgent
{
  private final transient FileSystem fs;
  private final transient Configuration conf;
  private final String localBasePath;

  private boolean syncCheckpoint = false;

  private AsyncFSStorageAgent()
  {
    super();
    fs = null;
    conf = null;
    localBasePath = null;
  }

  public AsyncFSStorageAgent(String path, Configuration conf)
  {
    this(".", path, conf);
  }

  public AsyncFSStorageAgent(String localBasePath, String path, Configuration conf)
  {
    super(path, conf);
    if (localBasePath == null) {
      this.localBasePath = "/tmp";
    }
    else {
      this.localBasePath = localBasePath;
    }
    logger.debug("Initialize storage agent with {}.", this.localBasePath);
    this.conf = conf == null ? new Configuration() : conf;
    try {
      fs = FileSystem.newInstance(this.conf);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void save(final Object object, final int operatorId, final long windowId) throws IOException
  {
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
    String operatorIdStr = String.valueOf(operatorId);
    File directory = new File(localBasePath, operatorIdStr);
    String window = Long.toHexString(windowId);
    Path lPath = new Path(path + Path.SEPARATOR + operatorIdStr + Path.SEPARATOR + System.currentTimeMillis() + TMP_FILE);
    FileUtil.copy(new File(directory, String.valueOf(windowId)), fs, lPath, true, conf);
    fileContext.rename(lPath, new Path(path + Path.SEPARATOR + operatorIdStr + Path.SEPARATOR + window), Options.Rename.OVERWRITE);
  }

  @Override
  public Object readResolve() throws ObjectStreamException
  {
    return new AsyncFSStorageAgent(this.localBasePath, this.path, null);
  }

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
