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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>FSAgent class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.5
 */
public class FSAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(FSAgent.class);
  protected FileSystem fs;
  protected Configuration conf;

  public FSAgent(Configuration conf)
  {
    this.conf = conf;
  }

  public void setup() throws IOException
  {
    fs = FileSystem.get(conf);
  }

  public void createFile(String path, byte[] content) throws IOException
  {
    createFile(new Path(path), content);
  }

  public FileSystem getFileSystem()
  {
    return fs;
  }

  public Configuration getConf()
  {
    return conf;
  }


  public void createFile(Path path, byte[] content) throws IOException
  {
    FSDataOutputStream os = fs.create(path);
    os.write(content);
    os.close();
  }

  public void deleteFile(String path) throws IOException
  {
    deleteFile(new Path(path));
  }

  public void deleteFile(Path path) throws IOException
  {
    fs.delete(path, false);
  }

  public byte[] readFullFileContent(Path path) throws IOException
  {
    DataInputStream is = new DataInputStream(fs.open(path));
    byte[] bytes = new byte[is.available()];
    is.readFully(bytes);
    is.close();
    return bytes;
  }

  public InputStreamReader openInputStreamReader(Path path) throws IOException
  {
    return new InputStreamReader(fs.open(path));
  }

  public List<String> listFiles(String dir) throws IOException
  {
    List<String> files = new ArrayList<String>();
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
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

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      files.add(lfs);
    }
    return files;
  }
}
