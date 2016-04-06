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
package com.datatorrent.stram.client;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.datatorrent.stram.util.FSUtil;

/**
 * <p>FSAgent class.</p>
 *
 * @since 0.3.5
 */
public class FSAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(FSAgent.class);
  protected FileSystem fileSystem;

  public FSAgent(FileSystem fs)
  {
    this.fileSystem = fs;
  }

  public void createFile(String path, byte[] content) throws IOException
  {
    createFile(new Path(path), content);
  }

  public FileSystem getFileSystem()
  {
    return fileSystem;
  }

  public void setFileSystem(FileSystem fileSystem)
  {
    this.fileSystem = fileSystem;
  }

  public void createDirectory(Path path) throws IOException
  {
    FSUtil.mkdirs(fileSystem, path);
  }

  public void createFile(Path path, byte[] content) throws IOException
  {
    try (FSDataOutputStream os = fileSystem.create(path)) {
      os.write(content);
    }
  }

  public void deleteFile(String path) throws IOException
  {
    deleteFile(new Path(path));
  }

  public void deleteFileRecursive(String path) throws IOException
  {
    deleteFileRecursive(new Path(path));
  }

  public void deleteFile(Path path) throws IOException
  {
    fileSystem.delete(path, false);
  }

  public void deleteFileRecursive(Path path) throws IOException
  {
    fileSystem.delete(path, true);
  }

  public byte[] readFullFileContent(Path path) throws IOException
  {
    DataInputStream is = new DataInputStream(fileSystem.open(path));
    byte[] bytes = new byte[is.available()];
    try {
      is.readFully(bytes);
    } finally {
      is.close();
    }
    return bytes;
  }

  public InputStreamReader openInputStreamReader(Path path) throws IOException
  {
    return new InputStreamReader(fileSystem.open(path));
  }

  public List<String> listFiles(String dir) throws IOException
  {
    List<String> files = new ArrayList<>();
    Path path = new Path(dir);

    FileStatus fileStatus = fileSystem.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(path, false);
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      files.add(lfs.getPath().getName());
    }
    return files;
  }

  public List<LocatedFileStatus> listFilesInfo(String dir) throws IOException
  {
    List<LocatedFileStatus> files = new ArrayList<>();
    Path path = new Path(dir);

    FileStatus fileStatus = fileSystem.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(path, false);
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      files.add(lfs);
    }
    return files;
  }

}
