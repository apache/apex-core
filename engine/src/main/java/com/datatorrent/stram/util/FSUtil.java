/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class FSUtil
{

  /**
   * Copied from FileUtil to transfer ownership
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
                              Configuration conf) throws IOException {
    Path src = srcStatus.getPath();
    //dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcStatus.isDirectory()) {
      //checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }

      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i], dstFS,
             new Path(dst, contents[i].getPath().getName()),
             deleteSource, overwrite, conf);
      }
    } else {
      InputStream in=null;
      OutputStream out = null;
      try {
        in = srcFS.open(src);
        out = dstFS.create(dst, overwrite);
        org.apache.hadoop.io.IOUtils.copyBytes(in, out, conf, true);
      } catch (IOException e) {
        org.apache.hadoop.io.IOUtils.closeStream(out);
        org.apache.hadoop.io.IOUtils.closeStream(in);
        throw e;
      }
    }

    // transfer owner
    dstFS.setOwner(dst, srcStatus.getOwner(), srcStatus.getGroup());

    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }

  }

}
