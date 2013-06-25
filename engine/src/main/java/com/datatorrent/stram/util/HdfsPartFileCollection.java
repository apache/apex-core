/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.stram.TupleRecorder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class HdfsPartFileCollection
{
  private transient FileSystem fs;
  private transient FSDataOutputStream partOutStr;
  private transient FSDataOutputStream indexOutStr;
  private transient FSDataOutputStream metaOs;
  private transient String localBasePath;
  public static final String INDEX_FILE = "index.txt";
  public static final String META_FILE = "meta.txt";
  protected int bytesPerPartFile = 100 * 1024;
  protected long millisPerPartFile = 30 * 60 * 1000; // 30 minutes
  protected int fileParts = 0;
  protected int partFileItemCount = 0;
  protected int partFileBytes = 0;
  protected long currentPartFileTimeStamp = 0;
  protected String basePath = ".";
  protected String hdfsFile;
  private boolean isLocalMode = false;
  private boolean syncRequested = false;
  private static final Logger logger = LoggerFactory.getLogger(TupleRecorder.class);

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
    fs = FileSystem.get(new Path(basePath).toUri(), new Configuration());

    Path pa = new Path(basePath, META_FILE);
    if (isLocalMode) {
      metaOs = new FSDataOutputStream(new FileOutputStream(localBasePath + "/" + META_FILE), null);
    }
    else {
      metaOs = fs.create(pa);
    }

    pa = new Path(basePath, INDEX_FILE);
    if (isLocalMode) {
      indexOutStr = new FSDataOutputStream(new FileOutputStream(localBasePath + "/" + INDEX_FILE), null);
    }
    else {
      indexOutStr = fs.create(pa);
    }
  }

  public void teardown()
  {
    logger.info("Closing down tuple recorder.");
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
    }
    catch (IOException ex) {
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
    }
    else {
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

  public void flushPartFile() throws IOException
  {
    partOutStr.hflush();
  }

  public void requestSync()
  {
    syncRequested = true;
  }

  public boolean isReadyTurnoverPartFile()
  {
    try {
      return syncRequested || (partOutStr.getPos() > bytesPerPartFile) || (currentPartFileTimeStamp + millisPerPartFile < System.currentTimeMillis());
    }
    catch (IOException ex) {
      return true;
    }
  }

  public void checkTurnover() throws IOException
  {
    partOutStr.hflush();
    if (isReadyTurnoverPartFile()) {
      turnover();
    }
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
    String extraInfo = getAndResetIndexExtraInfo();
    if (partFileBytes <= 0) {
      return;
    }
    try {
      String line = "F:" + hdfsFile + ":" + currentPartFileTimeStamp + "-" + System.currentTimeMillis() + ":" + partFileItemCount;
      if (extraInfo != null) {
        line += ":T:" + extraInfo;
      }
      line += "\n";

      indexOutStr.write(line.getBytes());
      indexOutStr.hflush();
      indexOutStr.hsync();
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  private void writeIndexEnd()
  {
    try {
      indexOutStr.write(("E\n").getBytes());
      indexOutStr.hflush();
      indexOutStr.hsync();
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  // to be overrided if user wants to include extra meta info for the current part file
  protected String getAndResetIndexExtraInfo()
  {
    return null;
  }

}
