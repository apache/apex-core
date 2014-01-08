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
  private final Configuration conf;

  private static final String FILE_LOG = "log";
  private static final String FILE_LOG_BACKUP = "log0";

  public FSRecoveryHandler(String appDir, Configuration conf)
  {
    this.dir = appDir + "/recovery";
    this.conf = conf;
  }

  @Override
  public DataOutputStream rotateLog() throws IOException
  {
    final Path logPath = new Path(dir + Path.SEPARATOR + FILE_LOG);
    final Path logBackupPath = new Path(dir + Path.SEPARATOR + FILE_LOG_BACKUP);

    FileSystem fs = FileSystem.get(logPath.toUri(), conf);

    if (fs.exists(logBackupPath)) {
      // log backup is purged on snapshot/restore
      throw new AssertionError("Snapshot state prior to log rotation: " + logBackupPath);
    }

    if (fs.exists(logPath)) {
      if (!fs.rename(logPath, logBackupPath)) {
        throw new IOException("Failed to rotate log: " + logPath);
      }
    }

    LOG.info("Creating {}", logPath);
    final FSDataOutputStream fsOutputStream;
    if (fs.getScheme().equals("file")) {
      // local FS does not support hflush and does not flush native stream
      fs.mkdirs(logPath.getParent());
      fsOutputStream = new FSDataOutputStream(new FileOutputStream(Path.getPathWithoutSchemeAndAuthority(logPath).toString()), null);
    } else {
      fsOutputStream = fs.create(logPath);
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
    final Path logPath = new Path(dir + Path.SEPARATOR + FILE_LOG);
    final Path logBackupPath = new Path(dir + Path.SEPARATOR + FILE_LOG_BACKUP);

    FileSystem fs = FileSystem.get(logPath.toUri(), conf);

    if (fs.exists(logBackupPath)) {
      // restore state prior to log replay
      throw new AssertionError("Restore state prior to reading log: " + logBackupPath);
    }

    if (fs.exists(logPath)) {
      LOG.info("Opening existing log ({})", logPath);
      return fs.open(logPath);
    } else {
      LOG.debug("No existing log ({})", logPath);
      return new DataInputStream(new ByteArrayInputStream(new byte[] {}));
    }

  }

  @Override
  public void save(Object state) throws IOException
  {
    Path backupPath = new Path(dir + "/snapshot0");
    Path path = new Path(dir + "/snapshot");
    FileSystem fs = FileSystem.get(path.toUri(), conf);

    if (fs.exists(backupPath)) {
      throw new IllegalStateException("Found previous backup " + backupPath);
    }

    if (fs.exists(path)) {
      LOG.debug("Backup {} to {}", path, backupPath);
      fs.rename(path, backupPath);
    }

    LOG.debug("Writing checkpoint to {}", path);
    final FSDataOutputStream fsOutputStream = fs.create(path);
    ObjectOutputStream oos = new ObjectOutputStream(fsOutputStream);
    oos.writeObject(state);
    oos.close();
    fsOutputStream.close();

    // remove snapshot backup
    if (fs.exists(backupPath) && !fs.delete(backupPath, false)) {
      throw new IOException("Failed to remove " + backupPath);
    }

    // remove log backup
    Path logBackup = new Path(dir + Path.SEPARATOR + FILE_LOG_BACKUP);
    if (fs.exists(logBackup) && !fs.delete(logBackup, false)) {
      throw new IOException("Failed to remove " + logBackup);
    }

  }

  @Override
  public Object restore() throws IOException
  {
    Path backupPath = new Path(dir + "/snapshot0");
    Path path = new Path(dir + "/snapshot");

    Path logBackupPath = new Path(dir + Path.SEPARATOR + FILE_LOG_BACKUP);
    Path logPath = new Path(dir + Path.SEPARATOR + FILE_LOG);

    FileContext fc = FileContext.getFileContext(FileSystem.get(path.toUri(), conf).getUri());

    if (fc.util().exists(backupPath)) {
      LOG.warn("Incomplete checkpoint, reverting to {}", backupPath);
      fc.delete(path, false);
      fc.rename(backupPath, path);

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
    }

    if (!fc.util().exists(path)) {
      LOG.debug("No existing checkpoint.");
      return null;
    }

    InputStream is = fc.open(path);
    ObjectInputStream ois = new ObjectInputStream(is);
    try {
      return ois.readObject();
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Failed to read checkpointed state", cnfe);
    }
  }

  public void writeConnectUri(String uri) throws IOException
  {
    Path path = new Path(dir + "/heartbeatUri");
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    DataOutputStream out = fs.create(path, true);
    out.write(uri.getBytes());
    out.close();
    LOG.info("Connect address: {} written to {} ", uri, path);
  }

  public String readConnectUri() throws IOException
  {
    Path path = new Path(dir + "/heartbeatUri");
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    DataInputStream in = fs.open(path);
    byte[] bytes = IOUtils.toByteArray(in);
    in.close();
    String uri = new String(bytes);
    LOG.info("Connect address: {} from {} ", uri, path);
    return uri;
  }

}
