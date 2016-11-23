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
package org.apache.apex.log.appender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.helpers.CountingQuietWriter;
import org.apache.log4j.helpers.LogLog;

/**
 * ApexRollingFileAppender extends {@link RollingFileAppender}
 * This appender doesn't keep renaming files when file reach max size.
 * The appender starts with <filename>.0 file when file reaches max size,
 * it creates next file <filename>.1 and so on till <filename>.MaxBackupIndex.
 * After that it will delete <filename>.0 and start creating log files in that
 * order again.
 * To identify current file, it creates <filename> symlink to current file.
 */
public class ApexRollingFileAppender extends RollingFileAppender
{
  private int currentFileIndex = 0;
  private String currentFileName = "";

  public synchronized void setFile(String fileName, boolean append, boolean bufferedIO, int bufferSize)
      throws IOException
  {
    currentFileName = fileName + "." + currentFileIndex;
    LogLog.debug("setFile called: " + currentFileName + ", " + append);

    // It does not make sense to have immediate flush and bufferedIO.
    if (bufferedIO) {
      setImmediateFlush(false);
    }

    reset();
    FileOutputStream ostream = null;
    try {
      //   attempt to create file
      ostream = new FileOutputStream(currentFileName, append);
    } catch (FileNotFoundException ex) {
      //   if parent directory does not exist then
      //      attempt to create it and try to create file
      //      see bug 9150
      String parentName = new File(currentFileName).getParent();
      if (parentName != null) {
        File parentDir = new File(parentName);
        if (!parentDir.exists() && parentDir.mkdirs()) {
          ostream = new FileOutputStream(currentFileName, append);
        } else {
          throw ex;
        }
      } else {
        throw ex;
      }
    }
    Writer fw = createWriter(ostream);
    if (bufferedIO) {
      fw = new BufferedWriter(fw, bufferSize);
    }

    if (new File(fileName).exists()) {
      Files.delete(Paths.get(fileName));
    }
    Files.createSymbolicLink(Paths.get(fileName), Paths.get(currentFileName));
    this.setQWForFiles(fw);
    this.fileName = fileName;
    this.fileAppend = append;
    this.bufferedIO = bufferedIO;
    this.bufferSize = bufferSize;
    writeHeader();
    LogLog.debug("setFile ended");
  }

  @Override
  public void rollOver()
  {
    if (qw != null) {
      long size = ((CountingQuietWriter)qw).getCount();
      LogLog.debug("rolling over count=" + size);
    }
    LogLog.debug("maxBackupIndex=" + maxBackupIndex);

    boolean deleteSucceeded = true;
    int nextFileIndex = (currentFileIndex + 1) % maxBackupIndex;
    String nextFileName = fileName + '.' + nextFileIndex;
    if (maxBackupIndex > 0) {
      // Delete the oldest file, to keep Windows happy.
      File file = new File(nextFileName);
      if (file.exists()) {
        deleteSucceeded = file.delete();
      }
      if (deleteSucceeded) {
        this.closeFile();
      } else { //   if file rename failed, reopen file with append = true
        try {
          this.setFile(fileName, true, bufferedIO, bufferSize);
        } catch (IOException e) {
          if (e instanceof InterruptedIOException) {
            Thread.currentThread().interrupt();
          }
          LogLog.error("setFile(" + fileName + ", true) call failed.", e);
        }
      }
    }
    currentFileIndex = nextFileIndex;
    //   if all renames were successful, then
    if (deleteSucceeded) {
      try {
        // This will also close the file. This is OK since multiple
        // close operations are safe.
        this.setFile(fileName, false, bufferedIO, bufferSize);
      } catch (IOException e) {
        if (e instanceof InterruptedIOException) {
          Thread.currentThread().interrupt();
        }
        LogLog.error("setFile(" + fileName + ", false) call failed.", e);
      }
    }
  }

  public String getCurrentFileName()
  {
    return currentFileName;
  }
}
