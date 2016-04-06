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
package com.datatorrent.stram;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;

import com.datatorrent.stram.util.FSUtil;

/**
 * <p>FSRecoveryHandler class.</p>
 *
 * @since 0.9.2
 */
public class FSRecoveryHandler implements StreamingContainerManager.RecoveryHandler
{
  private static final Logger LOG = LoggerFactory.getLogger(FSRecoveryHandler.class);
  private final Path basedir;
  private final Path logPath;
  private final Path logBackupPath;
  private final FileSystem fs;
  private final Path snapshotPath;
  private final Path snapshotBackupPath;
  private final Path heartbeatPath;

  public static final String FILE_LOG = "log";
  public static final String FILE_LOG_BACKUP = "log0";
  public static final String FILE_SNAPSHOT = "snapshot";
  public static final String FILE_SNAPSHOT_BACKUP = "snapshot0";
  private static final String DIRECTORY_RECOVERY = "recovery";
  private static final String FILE_HEARTBEATURI = "heartbeatUri";

  public FSRecoveryHandler(String appDir, Configuration conf) throws IOException
  {
    this.basedir = new Path(appDir, DIRECTORY_RECOVERY);
    fs = FileSystem.newInstance(this.basedir.toUri(), conf);

    logPath = new Path(basedir, FILE_LOG);
    logBackupPath = new Path(basedir, FILE_LOG_BACKUP);
    snapshotPath = new Path(basedir, FILE_SNAPSHOT);
    snapshotBackupPath = new Path(basedir, FILE_SNAPSHOT_BACKUP);
    heartbeatPath = new Path(basedir, FILE_HEARTBEATURI);
  }

  public String getDir()
  {
    return basedir.toUri().toString();
  }

  @Override
  public DataOutputStream rotateLog() throws IOException
  {

    if (fs.exists(logBackupPath)) {
      // log backup is purged on snapshot/restore
      throw new AssertionError("Snapshot state prior to log rotation: " + logBackupPath);
    }

    if (fs.exists(logPath)) {
      LOG.debug("Creating log backup {}", logBackupPath);
      if (!fs.rename(logPath, logBackupPath)) {
        throw new IOException("Failed to rotate log: " + logPath);
      }
    }

    LOG.info("Creating {}", logPath);
    final FSDataOutputStream fsOutputStream;
    String scheme = null;
    try {
      scheme = fs.getScheme();
    } catch (UnsupportedOperationException e) {
      LOG.warn("{} doesn't implement getScheme() method", fs.getClass().getName());
    }
    if ("file".equals(scheme)) {
      // local FS does not support hflush and does not flush native stream
      FSUtil.mkdirs(fs, logPath.getParent());
      fsOutputStream = new FSDataOutputStream(new FileOutputStream(Path.getPathWithoutSchemeAndAuthority(logPath).toString()), null);
    } else {
      fsOutputStream = fs.create(logPath);
    }

    DataOutputStream osWrapper = new DataOutputStream(fsOutputStream)
    {
      @Override
      public void flush() throws IOException
      {
        super.flush();
        fsOutputStream.hflush();
      }

      @Override
      public void close() throws IOException
      {
        LOG.debug("Closing {}", logPath);
        super.close();
      }
    };
    return osWrapper;
  }

  @Override
  public DataInputStream getLog() throws IOException
  {

    if (fs.exists(logBackupPath)) {
      // restore state prior to log replay
      throw new AssertionError("Restore state prior to reading log: " + logBackupPath);
    }

    if (fs.exists(logPath)) {
      LOG.debug("Opening existing log ({})", logPath);
      return fs.open(logPath);
    } else {
      LOG.debug("No existing log ({})", logPath);
      return new DataInputStream(new ByteArrayInputStream(new byte[] {}));
    }

  }

  @Override
  public void save(Object state) throws IOException
  {

    if (fs.exists(snapshotBackupPath)) {
      throw new IllegalStateException("Found previous backup " + snapshotBackupPath);
    }

    if (fs.exists(snapshotPath)) {
      LOG.debug("Backup {} to {}", snapshotPath, snapshotBackupPath);
      fs.rename(snapshotPath, snapshotBackupPath);
    }

    LOG.debug("Writing checkpoint to {}", snapshotPath);
    try (FSDataOutputStream fsOutputStream = fs.create(snapshotPath);
        ObjectOutputStream oos = new ObjectOutputStream(fsOutputStream)) {
      oos.writeObject(state);
    }
    // remove snapshot backup
    if (fs.exists(snapshotBackupPath) && !fs.delete(snapshotBackupPath, false)) {
      throw new IOException("Failed to remove " + snapshotBackupPath);
    }

    // remove log backup
    Path logBackup = new Path(basedir + Path.SEPARATOR + FILE_LOG_BACKUP);
    if (fs.exists(logBackup) && !fs.delete(logBackup, false)) {
      throw new IOException("Failed to remove " + logBackup);
    }

  }

  @Override
  public Object restore() throws IOException
  {
    FileContext fc = FileContext.getFileContext(fs.getUri());

    // recover from wherever it was left
    if (fc.util().exists(snapshotBackupPath)) {
      LOG.warn("Incomplete checkpoint, reverting to {}", snapshotBackupPath);
      fc.rename(snapshotBackupPath, snapshotPath, Rename.OVERWRITE);

      // combine logs (w/o append, create new file)
      Path tmpLogPath = new Path(basedir, "log.combined");
      try (FSDataOutputStream fsOut = fc.create(tmpLogPath, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE))) {
        try (FSDataInputStream fsIn = fc.open(logBackupPath)) {
          IOUtils.copy(fsIn, fsOut);
        }

        try (FSDataInputStream fsIn = fc.open(logPath)) {
          IOUtils.copy(fsIn, fsOut);
        }
      }

      fc.rename(tmpLogPath, logPath, Rename.OVERWRITE);
      fc.delete(logBackupPath, false);
    } else {
      // we have log backup, but no checkpoint backup
      // failure between log rotation and writing checkpoint
      if (fc.util().exists(logBackupPath)) {
        LOG.warn("Found {}, did checkpointing fail?", logBackupPath);
        fc.rename(logBackupPath, logPath, Rename.OVERWRITE);
      }
    }

    if (!fc.util().exists(snapshotPath)) {
      LOG.debug("No existing checkpoint.");
      return null;
    }

    LOG.debug("Reading checkpoint {}", snapshotPath);
    InputStream is = fc.open(snapshotPath);
    // indeterministic class loading behavior
    // http://stackoverflow.com/questions/9110677/readresolve-not-working-an-instance-of-guavas-serializedform-appears
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (ObjectInputStream ois = new ObjectInputStream(is)
    {
      @Override
      protected Class<?> resolveClass(ObjectStreamClass objectStreamClass)
          throws IOException, ClassNotFoundException
      {
        return Class.forName(objectStreamClass.getName(), true, loader);
      }
    }) {
      return ois.readObject();
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Failed to read checkpointed state", cnfe);
    }
  }

  public void writeConnectUri(String uri) throws IOException
  {
    try (DataOutputStream out = fs.create(heartbeatPath, true)) {
      out.write(uri.getBytes());
    }
    LOG.debug("Connect address: {} written to {} ", uri, heartbeatPath);
  }

  public String readConnectUri() throws IOException
  {
    byte[] bytes;

    DataInputStream in = fs.open(heartbeatPath);
    try {
      bytes = IOUtils.toByteArray(in);
    } finally {
      in.close();
    }

    String uri = new String(bytes);
    LOG.debug("Connect address: {} from {} ", uri, heartbeatPath);
    return uri;
  }

  @Override
  @SuppressWarnings("FinalizeDeclaration")
  protected void finalize() throws Throwable
  {
    try {
      fs.close();
    } finally {
      super.finalize();
    }
  }

}
