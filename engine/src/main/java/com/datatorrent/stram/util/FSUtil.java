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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * <p>FSUtil class.</p>
 *
 * @since 1.0.1
 */
public class FSUtil
{
  private static final Logger LOG = LoggerFactory.getLogger(FSUtil.class);

  /**
   * Copied from FileUtil to transfer ownership
   *
   * @param srcFS
   * @param srcStatus
   * @param dstFS
   * @param dst
   * @param deleteSource
   * @param overwrite
   * @param conf
   * @return
   * @throws IOException
   */
  public static boolean copy(FileSystem srcFS, FileStatus srcStatus,
      FileSystem dstFS, Path dst,
      boolean deleteSource,
      boolean overwrite,
      Configuration conf) throws IOException
  {
    Path src = srcStatus.getPath();
    //dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcStatus.isDirectory()) {
      //checkDependencies(srcFS, src, dstFS, dst);
      if (!mkdirs(dstFS, dst)) {
        return false;
      }

      FileStatus[] contents = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i], dstFS,
            new Path(dst, contents[i].getPath().getName()),
            deleteSource, overwrite, conf);
      }
    } else {
      try (InputStream in = srcFS.open(src);
          OutputStream out = dstFS.create(dst, overwrite)) {
        org.apache.hadoop.io.IOUtils.copyBytes(in, out, conf, true);
      }
    }

    // TODO: change group and limit write to group
    if (srcStatus.isDirectory()) {
      dstFS.setPermission(dst, new FsPermission((short)0777));
    } else {
      dstFS.setPermission(dst, new FsPermission((short)0777)/*"ugo+w"*/);
    }
    //dstFS.setOwner(dst, null, srcStatus.getGroup());

/*
    try {
      // transfer owner
      // DOES NOT WORK only super user can change file owner
      dstFS.setOwner(dst, srcStatus.getOwner(), srcStatus.getGroup());
    } catch (IOException e) {
      LOG.warn("Failed to change owner on {} to {}", dst, srcStatus.getOwner(), e);
      throw e;
    }
*/
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }

  }

  public static void setPermission(FileSystem fs, Path dst, FsPermission permission) throws IOException
  {
    FileStatus[] contents = fs.listStatus(dst);
    for (int i = 0; i < contents.length; i++) {
      fs.setPermission(contents[i].getPath(), permission);
    }
    fs.setPermission(dst, permission);
  }

  public static boolean mkdirs(FileSystem fs, Path dest) throws IOException
  {
    try {
      return fs.mkdirs(dest);
    } catch (IOException e) {
      // some file system (MapR) throw exception if folder exists
      if (!fs.exists(dest)) {
        throw e;
      } else {
        return false;
      }
    }
  }

  /**
   * Download the file from dfs to local file.
   *
   * @param fs
   * @param destinationFile
   * @param dfsFile
   * @param conf
   * @return
   * @throws IOException
   */
  public static File copyToLocalFileSystem(FileSystem fs, String destinationPath, String destinationFile, String dfsFile, Configuration conf)
      throws IOException
  {
    File destinationDir = new File(destinationPath);
    if (!destinationDir.exists() && !destinationDir.mkdirs()) {
      throw new RuntimeException("Unable to create local directory");
    }
    try (RawLocalFileSystem localFileSystem = new RawLocalFileSystem()) {
      // allow app user to access local dir
      FsPermission permissions = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
      localFileSystem.setPermission(new Path(destinationDir.getAbsolutePath()), permissions);

      Path dfsFilePath = new Path(dfsFile);
      File localFile = new File(destinationDir, destinationFile);
      FileUtil.copy(fs, dfsFilePath, localFile, false, conf);
      // set permissions on actual file to be read-only for user
      permissions = new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE);
      localFileSystem.setPermission(new Path(localFile.getAbsolutePath()), permissions);
      return localFile;
    }
  }

}
