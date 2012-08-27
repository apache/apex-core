/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.dag.AbstractInputNode;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;
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

public abstract class AbstractHDFSInputStream extends AbstractInputNode implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractHDFSInputStream.class);
  protected FSDataInputStream input;
  private boolean skipEndStream = false;

  /**
   * 
   * @return boolean
   */
  public boolean isSkipEndStream() {
    return skipEndStream;
  }

  /**
   * 
   * @param skip 
   */
  public void setSkipEndStream(boolean skip) {
    this.skipEndStream = skip;
  }

  /**
   * 
   * @param config 
   */
  @Override
  public void setup(NodeConfiguration config)
  {
    super.setup(config);

    try {
      FileSystem fs = FileSystem.get(config);
      Path filepath = new Path(config.get("filepath"));
      input = fs.open(filepath);
    }
    catch (IOException ex) {
      logger.error(ex.getLocalizedMessage());
    }

  }

  /**
   * 
   */
  @Override
  public void activate(NodeContext context)
  {
    super.activate(context);
    Thread t = new Thread(this);
    t.start();
  }

  /**
   * 
   */
  @Override
  public void run()
  {
    logger.debug("ready to read hdfs file");
    Object o;
    while ((o = getRecord(input)) != null) {
      emit(o);
    }

    if (!skipEndStream) {
      emit(new Tuple(Buffer.Data.DataType.END_STREAM));
    } else {
      logger.info("Skipping endStream.");
    }
  }

  /**
   * 
   */
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

  /**
   * 
   * @param input
   * @return 
   */
  public abstract Object getRecord(FSDataInputStream input);
}
