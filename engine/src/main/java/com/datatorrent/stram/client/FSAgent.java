/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.util.FSUtil;

/**
 * <p>FSAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
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
    FSDataOutputStream os = fileSystem.create(path);
    try {
      os.write(content);
    }
    finally {
      os.close();
    }
  }

  public void deleteFile(String path) throws IOException
  {
    deleteFile(new Path(path));
  }

  public void deleteFile(Path path) throws IOException
  {
    fileSystem.delete(path, false);
  }

  public byte[] readFullFileContent(Path path) throws IOException
  {
    DataInputStream is = new DataInputStream(fileSystem.open(path));
    byte[] bytes = new byte[is.available()];
    try {
      is.readFully(bytes);
    }
    finally {
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
    List<String> files = new ArrayList<String>();
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
    List<LocatedFileStatus> files = new ArrayList<LocatedFileStatus>();
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
