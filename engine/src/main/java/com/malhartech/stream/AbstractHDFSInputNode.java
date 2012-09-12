/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.dag.AbstractInputNode;
import com.malhartech.dag.Node;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
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
public abstract class AbstractHDFSInputNode extends AbstractInputNode implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractHDFSInputNode.class);
  protected FSDataInputStream input;
  private boolean skipEndStream = false;

  /**
   *
   * @return boolean
   */
  public boolean isSkipEndStream()
  {
    return skipEndStream;
  }

  /**
   *
   * @param skip
   */
  public void setSkipEndStream(boolean skip)
  {
    this.skipEndStream = skip;
  }

  /**
   *
   * @param config
   */
  @Override
  public void setup(NodeConfiguration config) throws Exception
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
  public void run()
  {
    logger.debug("ready to read hdfs file");
    try {
      while (true) {
        emitRecord(input);
      }
    }
    catch (Exception e) {
      logger.info("Exception on HDFS Input: {}", e.getLocalizedMessage());
      if (skipEndStream) {
        logger.info("Skipping end stream as requested");
      }
      else {
        logger.info("Ending the stream");
        Class<? extends Node> clazz = this.getClass();
        NodeAnnotation na = clazz.getAnnotation(NodeAnnotation.class);
        if (na != null) {
          PortAnnotation[] ports = na.ports();
          for (PortAnnotation pa: ports) {
            if (pa.type() == PortType.OUTPUT || pa.type() == PortType.BIDI) {
              emit(pa.name(), new Tuple(Buffer.Data.DataType.END_STREAM));
            }
          }
        }
      }
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

  protected abstract void emitRecord(FSDataInputStream input);
}
