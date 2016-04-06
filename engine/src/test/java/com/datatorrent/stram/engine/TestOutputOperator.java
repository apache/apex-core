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
package com.datatorrent.stram.engine;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.tuple.Tuple;

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
  public final transient InputPort<Object> inport = new DefaultInputPort<Object>()
  {
    @Override
    public final void process(Object payload)
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
        } else {
          fs.delete(filepath, true);
          output = fs.create(filepath);
        }
      } else {
        output = fs.create(filepath);
      }
    } catch (IOException iOException) {
      logger.debug(iOException.getLocalizedMessage());
      throw new RuntimeException(iOException.getCause());
    } catch (IllegalArgumentException illegalArgumentException) {
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
    } catch (IOException ex) {
      logger.info("", ex);
    }

    fs = null;
    filepath = null;
    append = false;
    super.teardown();
  }

  /**
   * @param t the value of t
   */
  private void processInternal(Object t)
  {
    logger.debug("received: " + t);
    if (t instanceof Tuple) {
      logger.debug("ignoring tuple " + t);
    } else {
      byte[] serialized = ("" + t + "\n").getBytes();
      try {
        output.write(serialized);
      } catch (IOException ex) {
        logger.info("", ex);
      }
    }
  }
}
