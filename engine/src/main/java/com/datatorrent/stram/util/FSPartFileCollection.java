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
package com.datatorrent.stram.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * <p>FSPartFileCollection class.</p>
 *
 * @since 0.3.2
 */
public class FSPartFileCollection
{
  private transient FileSystem fs;
  private transient FSDataOutputStream partOutStr;
  private transient FSDataOutputStream indexOutStr;
  private transient FSDataOutputStream metaOs;
  private transient String localBasePath;
  public static final String INDEX_FILE = "index.txt";
  public static final String META_FILE = "meta.txt";
  protected int bytesPerPartFile = 1024 * 1024;
  protected long millisPerPartFile = 60 * 60 * 1000; // 60 minutes
  protected int fileParts = 0;
  protected int partFileItemCount = 0;
  protected int partFileBytes = 0;
  protected long currentPartFileTimeStamp = 0;
  protected String basePath = ".";
  protected String hdfsFile;
  private boolean isLocalMode = false;
  private boolean syncRequested = false;

  public void setBytesPerPartFile(int bytes)
  {
    this.bytesPerPartFile = bytes;
  }

  public void setMillisPerPartFile(long millis)
  {
    this.millisPerPartFile = millis;
  }

  public void setLocalMode(boolean isLocalMode)
  {
    this.isLocalMode = isLocalMode;
  }

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public String getBasePath()
  {
    return this.basePath;
  }

  public void setup() throws IOException
  {
    if (basePath.startsWith("file:")) {
      isLocalMode = true;
      localBasePath = basePath.substring(5);
      (new File(localBasePath)).mkdirs();
    }
    fs = FileSystem.newInstance(new Path(basePath).toUri(), new Configuration());

    Path pa = new Path(basePath, META_FILE);
    if (isLocalMode) {
      metaOs = new FSDataOutputStream(new FileOutputStream(localBasePath + "/" + META_FILE), null);
    } else {
      metaOs = fs.create(pa);
    }

    pa = new Path(basePath, INDEX_FILE);
    if (isLocalMode) {
      indexOutStr = new FSDataOutputStream(new FileOutputStream(localBasePath + "/" + INDEX_FILE), null);
    } else {
      indexOutStr = fs.create(pa);
    }
  }

  public void teardown()
  {
    logger.info("Closing hdfs part collection.");
    try {
      if (metaOs != null) {
        metaOs.close();
      }
      if (partOutStr != null) {
        logger.debug("Closing part file");
        partOutStr.close();
        if (indexOutStr != null) {
          writeIndex();
        }
      }
      if (indexOutStr != null) {
        writeIndexEnd();
        indexOutStr.close();
      }
      fs.close();
    } catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  private void openNewPartFile() throws IOException
  {
    hdfsFile = "part" + fileParts + ".txt";
    Path path = new Path(basePath, hdfsFile);
    logger.debug("Opening new part file: {}", hdfsFile);
    if (isLocalMode) {
      partOutStr = new FSDataOutputStream(new FileOutputStream(localBasePath + "/" + hdfsFile), null);
    } else {
      partOutStr = fs.create(path);
    }
    fileParts++;
    currentPartFileTimeStamp = System.currentTimeMillis();
    partFileItemCount = 0;
    partFileBytes = 0;
  }

  public void writeMetaData(byte[] bytes) throws IOException
  {
    metaOs.write(bytes);
    metaOs.hflush();
  }

  public void writeDataItem(byte[] bytes, boolean incrementItemCount) throws IOException
  {
    if (partOutStr == null) {
      openNewPartFile();
    }
    partOutStr.write(bytes);
    partFileBytes += bytes.length;
    if (incrementItemCount) {
      partFileItemCount++;
    }
  }

  public void requestSync()
  {
    syncRequested = true;
  }

  public boolean isReadyTurnoverPartFile()
  {
    try {
      return (syncRequested || (partOutStr.getPos() > bytesPerPartFile) ||
          (currentPartFileTimeStamp + millisPerPartFile < System.currentTimeMillis())) && partOutStr.getPos() > 0;
    } catch (IOException ex) {
      return true;
    }
  }

  public boolean flushData() throws IOException
  {
    if (partOutStr != null) {
      partOutStr.hflush();
      if (isReadyTurnoverPartFile()) {
        turnover();
        return true;
      }
    }
    return false;
  }

  private void turnover() throws IOException
  {
    partOutStr.close();
    partOutStr = null;
    writeIndex();
    syncRequested = false;
  }

  private void writeIndex()
  {
    if (partFileBytes <= 0) {
      return;
    }
    try {
      String line = getLatestIndexLine();
      resetIndexExtraInfo();
      indexOutStr.write(line.getBytes());
      indexOutStr.hflush();
      indexOutStr.hsync();
    } catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  public String getLatestIndexLine()
  {
    String extraInfo = getIndexExtraInfo();

    String line = "F:" + hdfsFile + ":" + currentPartFileTimeStamp + "-" + System.currentTimeMillis() + ":" + partFileItemCount;
    if (extraInfo != null) {
      line += ":T:" + extraInfo;
    }
    line += "\n";
    return line;
  }

  private void writeIndexEnd()
  {
    try {
      indexOutStr.write(("E\n").getBytes());
      indexOutStr.hflush();
      indexOutStr.hsync();
    } catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  // to be overrided if user wants to include extra meta info for the current part file
  protected String getIndexExtraInfo()
  {
    return null;
  }

  // to be overrided if user wants to reset extra meta info for the current part file
  protected void resetIndexExtraInfo()
  {
  }

  private static final Logger logger = LoggerFactory.getLogger(FSPartFileCollection.class);
}
