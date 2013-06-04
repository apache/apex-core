/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.tuple.Tuple;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes stringified tuple to a file stream.
 * Used to verify data flow in test.
 */
public class TestOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(TestOutputOperator.class);
  private boolean append;
  public String pathSpec;
  private transient FSDataOutputStream output;
  private transient FileSystem fs;
  private transient Path filepath;
  @InputPortFieldAnnotation(name = "inputPort")
  final public transient InputPort<Object> inport = new DefaultInputPort<Object>(this)
  {
    @Override
    final public void process(Object payload)
    {
      processInternal(payload);
    }
  };

  public void setAppend(boolean flag)
  {
    append = flag;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      fs = FileSystem.get(new Configuration());
      if (pathSpec == null) {
        throw new IllegalArgumentException("pathSpec not specified.");
      }

      filepath = new Path(pathSpec);

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
      throw new RuntimeException(iOException.getCause());
    }
    catch (IllegalArgumentException illegalArgumentException) {
      logger.debug(illegalArgumentException.getLocalizedMessage());
      throw new RuntimeException(illegalArgumentException);
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
  private void processInternal(Object t)
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
