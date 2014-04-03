/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
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
import java.util.EnumSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>FSRecoveryHandler class.</p>
 *
 * @since 0.9.2
 */
public class FSRecoveryHandler implements StreamingContainerManager.RecoveryHandler
{
  private final static Logger LOG = LoggerFactory.getLogger(FSRecoveryHandler.class);
  private final String dir;
  private final Path logPath ;
  private final Path logBackupPath;
  private final FileSystem logfs;
  private final Path snapshotPath;
  private final Path snapshotBackupPath;
  private final FileSystem snapshotFs;
  private final Path heartbeatPath;
  private final FileSystem heartbeatFs;

  public static final String FILE_LOG = "log";
  public static final String FILE_LOG_BACKUP = "log0";
  public static final String FILE_SNAPSHOT = "snapshot";
  public static final String FILE_SNAPSHOT_BACKUP = "snapshot0";

  public FSRecoveryHandler(String appDir, Configuration conf) throws IOException
  {
    this.dir = appDir + "/recovery";
    
    logPath = new Path(dir + Path.SEPARATOR + FILE_LOG);
    logBackupPath = new Path(dir + Path.SEPARATOR + FILE_LOG_BACKUP);
    snapshotPath = new Path(dir + "/snapshot");
    snapshotBackupPath = new Path(dir + "/" + FILE_SNAPSHOT_BACKUP);
    
    logfs = FileSystem.newInstance(logPath.toUri(), conf);
    snapshotFs = FileSystem.newInstance(snapshotPath.toUri(), conf);
    
    heartbeatPath = new Path(dir + "/heartbeatUri");
    heartbeatFs = FileSystem.newInstance(heartbeatPath.toUri(), conf);
  }

  public String getDir()
  {
    return dir;
  }

  @Override
  public DataOutputStream rotateLog() throws IOException
  {

    if (logfs.exists(logBackupPath)) {
      // log backup is purged on snapshot/restore
      throw new AssertionError("Snapshot state prior to log rotation: " + logBackupPath);
    }

    if (logfs.exists(logPath)) {
      LOG.debug("Creating log backup {}", logBackupPath);
      if (!logfs.rename(logPath, logBackupPath)) {
        throw new IOException("Failed to rotate log: " + logPath);
      }
    }

    LOG.info("Creating {}", logPath);
    final FSDataOutputStream fsOutputStream;
    if (logfs.getScheme().equals("file")) {
      // local FS does not support hflush and does not flush native stream
      logfs.mkdirs(logPath.getParent());
      fsOutputStream = new FSDataOutputStream(new FileOutputStream(Path.getPathWithoutSchemeAndAuthority(logPath).toString()), null);
    } else {
      fsOutputStream = logfs.create(logPath);
    }

    DataOutputStream osWrapper = new DataOutputStream(fsOutputStream) {
      @Override
      public void flush() throws IOException
      {
        super.flush();
        fsOutputStream.hflush();
      }

      @Override
      public void close() throws IOException
      {
        LOG.info("Closing {}", logPath);
        super.close();
      }
    };
    return osWrapper;
  }

  @Override
  public DataInputStream getLog() throws IOException
  {

    if (logfs.exists(logBackupPath)) {
      // restore state prior to log replay
      throw new AssertionError("Restore state prior to reading log: " + logBackupPath);
    }

    if (logfs.exists(logPath)) {
      LOG.info("Opening existing log ({})", logPath);
      return logfs.open(logPath);
    } else {
      LOG.debug("No existing log ({})", logPath);
      return new DataInputStream(new ByteArrayInputStream(new byte[] {}));
    }

  }

  @Override
  public void save(Object state) throws IOException
  {

    if (snapshotFs.exists(snapshotBackupPath)) {
      throw new IllegalStateException("Found previous backup " + snapshotBackupPath);
    }

    if (snapshotFs.exists(snapshotPath)) {
      LOG.debug("Backup {} to {}", snapshotPath, snapshotBackupPath);
      snapshotFs.rename(snapshotPath, snapshotBackupPath);
    }

    LOG.debug("Writing checkpoint to {}", snapshotPath);
    final FSDataOutputStream fsOutputStream = snapshotFs.create(snapshotPath);
    ObjectOutputStream oos = new ObjectOutputStream(fsOutputStream);
    oos.writeObject(state);
    oos.close();
    fsOutputStream.close();

    // remove snapshot backup
    if (snapshotFs.exists(snapshotBackupPath) && !snapshotFs.delete(snapshotBackupPath, false)) {
      throw new IOException("Failed to remove " + snapshotBackupPath);
    }

    // remove log backup
    Path logBackup = new Path(dir + Path.SEPARATOR + FILE_LOG_BACKUP);
    if (snapshotFs.exists(logBackup) && !snapshotFs.delete(logBackup, false)) {
      throw new IOException("Failed to remove " + logBackup);
    }

  }

  @Override
  public Object restore() throws IOException
  {
    FileContext fc = FileContext.getFileContext(snapshotFs.getUri());

    // recover from wherever it was left
    if (fc.util().exists(snapshotBackupPath)) {
      LOG.warn("Incomplete checkpoint, reverting to {}", snapshotBackupPath);
      fc.rename(snapshotBackupPath, snapshotPath, Rename.OVERWRITE);

      // combine logs (w/o append, create new file)
      Path tmpLogPath = new Path(dir + "/log.combined");
      FSDataOutputStream fsOut = fc.create(tmpLogPath, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
      FSDataInputStream fsIn = fc.open(logBackupPath);
      IOUtils.copy(fsIn, fsOut);
      fsIn.close();

      fsIn = fc.open(logPath);
      IOUtils.copy(fsIn, fsOut);
      fsIn.close();

      fsOut.close();
      fc.rename(tmpLogPath, logPath, Rename.OVERWRITE);
      fc.delete(logBackupPath, false);
    } else {
      // we have log backup, but no checkpoint backup
      // failure between log rotation and writing checkpoint
      if (fc.util().exists(logBackupPath)) {
        LOG.warn("Found {}, did checkpointing fail?");
        fc.rename(logBackupPath, logPath, Rename.OVERWRITE);
      }

    }

    if (!fc.util().exists(snapshotPath)) {
      LOG.debug("No existing checkpoint.");
      return null;
    }

    InputStream is = fc.open(snapshotPath);
    ObjectInputStream ois = new ObjectInputStream(is);
    try {
      return ois.readObject();
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Failed to read checkpointed state", cnfe);
    }
  }

  public void writeConnectUri(String uri) throws IOException
  {
    DataOutputStream out = heartbeatFs.create(heartbeatPath, true);
    out.write(uri.getBytes());
    out.close();
    LOG.info("Connect address: {} written to {} ", uri, heartbeatPath);
  }

  public String readConnectUri() throws IOException
  {
    DataInputStream in = heartbeatFs.open(heartbeatPath);
    byte[] bytes = IOUtils.toByteArray(in);
    in.close();
    String uri = new String(bytes);
    LOG.info("Connect address: {} from {} ", uri, heartbeatPath);
    return uri;
  }
  
  @Override
  protected void finalize() throws Throwable
  {
    super.finalize();
    logfs.close();
    snapshotFs.close();
    heartbeatFs.close();
  }

}
