/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.*;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/**
 * Adapter for writing to HDFS<p>
 * <br>
 * Serializes tuples into a HDFS file<br>
 * Currently all tuples are written to a single HDFS file<br>
 * Future enhancements include options to write into a time slot/windows based files<br>
 * <br>
 *
 */
@NodeAnnotation(ports= {
  @PortAnnotation(name = Component.INPUT, type = PortAnnotation.PortType.INPUT)
})
public class HDFSOutputNode extends AbstractNode
{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(HDFSOutputNode.class);
  private FSDataOutputStream output;
  private SerDe serde; // it was taken from context before, but now what, it's not a stream but a node!
  private FileSystem fs;
  private Path filepath;
  private boolean append;

  /**
   *
   * @param config
   */
  @Override
  public void setup(NodeConfiguration config) throws FailedOperationException
  {
    try {
      fs = FileSystem.get(config);
      filepath = new Path(config.get("filepath"));
      append = config.getBoolean("append", true);

      if (fs.exists(filepath)) {
        if (append) {
          output = fs.append(filepath);
        }
        else {
          fs.delete(filepath, true);
          output = fs.create(filepath);
        }
      }
      else {
        output = fs.create(filepath);
      }
    }
    catch (IOException ex) {
      logger.debug(ex.getLocalizedMessage());
      throw new FailedOperationException(ex.getCause());
    }
  }

  @Override
  public void teardown()
  {
    try {
      output.close();
      output = null;
    }
    catch (IOException ex) {
      logger.info("", ex);
    }

    serde = null;

    fs = null;
    filepath = null;
    append = false;
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t)
  {
    if (t instanceof Tuple) {
      logger.debug("ignoring tuple " + t);
    }
    else {
      byte[] serialized = serde.toByteArray(t);
      try {
        output.write(serialized);
      }
      catch (IOException ex) {
        logger.info("", ex);
      }
    }
  }
}
