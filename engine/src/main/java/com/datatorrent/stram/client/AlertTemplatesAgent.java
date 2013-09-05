/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class AlertTemplatesAgent extends StramAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(AlertTemplatesAgent.class);

  public static String getAlertTemplatesDirectory(String stramRoot)
  {
    return stramRoot + Path.SEPARATOR + "alertTemplates";
  }

  public void createAlertTemplate(String stramRoot, String name, String content) throws IOException
  {
    String dir = getAlertTemplatesDirectory(stramRoot);
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    createFile(path, content.getBytes());
  }

  public void deleteAlertTemplate(String stramRoot, String name) throws IOException
  {
    String dir = getAlertTemplatesDirectory(stramRoot);
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    path = new Path(path, name);
    deleteFile(path);
  }

  public Map<String, String> listAlertTemplates(String stramRoot) throws IOException
  {
    String dir = getAlertTemplatesDirectory(stramRoot);
    Map<String, String> map = new HashMap<String, String>();
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      FSDataInputStream is = fs.open(lfs.getPath());
      byte[] bytes = new byte[is.available()];
      is.readFully(bytes);
      String content = new String(bytes);
      map.put(lfs.getPath().getName(), content);
    }
    return map;
  }

  public String getAlertTemplate(String stramRoot, String name) throws IOException
  {
    String dir = getAlertTemplatesDirectory(stramRoot);
    Path path = new Path(dir);

    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDirectory()) {
      throw new FileNotFoundException("Cannot read directory " + dir);
    }
    FSDataInputStream is = fs.open(new Path(path, name));
    byte[] bytes = new byte[is.available()];
    is.readFully(bytes);
    return new String(bytes);
  }

}
