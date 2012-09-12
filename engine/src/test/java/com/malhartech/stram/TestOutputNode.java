/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.Tuple;

/**
 * Writes stringified tuple to a file stream.
 * Used to verify data flow in test.
 */
@NodeAnnotation(
    ports = {
  @PortAnnotation(name = TestOutputNode.PORT_INPUT, type = PortType.INPUT)
})
public class TestOutputNode extends AbstractNode
{
  private static final Logger LOG = LoggerFactory.getLogger(TestOutputNode.class);
  /**
   * The path name for the output file.
   */
  public static final String P_FILEPATH = "filepath";
  public static final String PORT_INPUT = "inputPort";
  private transient FSDataOutputStream output;
  private transient FileSystem fs;
  private transient Path filepath;
  private boolean append;

  @Override
  public void setup(NodeConfiguration config) throws Exception
  {
    super.setup(config);
    fs = FileSystem.get(config);
    String pathSpec = config.get(P_FILEPATH);
    if (pathSpec == null) {
      throw new IllegalArgumentException(P_FILEPATH + " not specified.");
    }
    filepath = new Path(pathSpec);
    append = config.getBoolean("append", false);

    LOG.info("output file: " + filepath);
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

  @Override
  public void teardown()
  {
    try {
      output.close();
      output = null;
    }
    catch (IOException ex) {
      LOG.info("", ex);
    }

    fs = null;
    filepath = null;
    append = false;
    super.teardown();
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t)
  {
    LOG.debug("received: " + t);
    if (t instanceof Tuple) {
      LOG.debug("ignoring tuple " + t);
    }
    else {
      byte[] serialized = ("" + t + "\n").getBytes();
      try {
        output.write(serialized);
      }
      catch (IOException ex) {
        LOG.info("", ex);
      }
    }
  }
}
