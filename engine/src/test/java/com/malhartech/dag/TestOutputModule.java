/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.api.Sink;
import com.malhartech.dag.GenericNode;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.OperatorConfiguration;
import com.malhartech.dag.Tuple;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes stringified tuple to a file stream.
 * Used to verify data flow in test.
 */
@ModuleAnnotation(
    ports = {
  @PortAnnotation(name = TestOutputModule.PORT_INPUT, type = PortType.INPUT)
})
public class TestOutputModule extends GenericNode implements Sink
{
  private static final Logger logger = LoggerFactory.getLogger(TestOutputModule.class);
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
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    try {
      fs = FileSystem.get(config);
      String pathSpec = config.get(P_FILEPATH);
      if (pathSpec == null) {
        throw new IllegalArgumentException(P_FILEPATH + " not specified.");
      }
      filepath = new Path(pathSpec);
      append = config.getBoolean("append", false);

      logger.info("output file: " + filepath);
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
    catch (IOException iOException) {
      logger.debug(iOException.getLocalizedMessage());
      throw new FailedOperationException(iOException.getCause());
    }
    catch (IllegalArgumentException illegalArgumentException) {
      logger.debug(illegalArgumentException.getLocalizedMessage());
      throw new FailedOperationException(illegalArgumentException);
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
    logger.debug("received: " + t);
    if (t instanceof Tuple) {
      logger.debug("ignoring tuple " + t);
    }
    else {
      byte[] serialized = ("" + t + "\n").getBytes();
      try {
        output.write(serialized);
      }
      catch (IOException ex) {
        logger.info("", ex);
      }
    }
  }
}
