/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */

/**
  * Input Adapter for reading from HDFS<p>
  * <br>
  * Extends AbstractInputAdapter<br>
  * Users need to implement getRecord to get HDFS input adapter to work as per their choice<br>
  * <br>
 */

public abstract class AbstractHDFSInputStream extends AbstractInputAdapter implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractHDFSInputStream.class);
  protected FSDataInputStream input;
  private boolean skipEndStream = false;

  public boolean isSkipEndStream() {
    return skipEndStream;
  }

  public void setSkipEndStream(boolean skip) {
    this.skipEndStream = skip;
  }

  @Override
  public void setup(StreamConfiguration config)
  {
    try {
      FileSystem fs = FileSystem.get(config);
      Path filepath = new Path(config.get("filepath"));
      input = fs.open(filepath);
    }
    catch (IOException ex) {
      logger.error(ex.getLocalizedMessage());
    }

  }

  @Override
  public void activate(StreamContext context)
  {
    Thread t = new Thread(this);
    t.start();
  }

  @Override
  public void run()
  {
    logger.debug("ready to read hdfs file");
    Object o;
    while ((o = getRecord(input)) != null) {
      emit(o);
    }

    if (!skipEndStream) {
      endStream();
    } else {
      logger.info("Skipping endStream.");
    }
  }

  @Override
  public void teardown()
  {
    try {
      input.close();
    }
    catch (IOException ex) {
      logger.error(ex.getLocalizedMessage());
    }
  }

  public abstract Object getRecord(FSDataInputStream input);
}
