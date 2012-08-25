/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

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


public class HDFSOutputStream
  implements Stream, Sink
{
  private static org.slf4j.Logger LOG = LoggerFactory.getLogger(HDFSOutputStream.class);
  private StreamContext context;
  private FSDataOutputStream output;

  /**
   * 
   * @param config 
   */
  @Override
  public void setup(StreamConfiguration config)
  {
    try {
      FileSystem fs = FileSystem.get(config);
      Path filepath = new Path(config.get("filepath"));

      if (fs.exists(filepath)) {
        if (config.getBoolean("append", true)) {
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
      LOG.info("", ex);
    }

  }

  /**
   * 
   * @param context 
   */
  @Override
  public void setContext(StreamContext context)
  {
    this.context = context;
  }

  /**
   * 
   */
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
  }

  /**
   * 
   * @param t 
   */
  @Override
  public void doSomething(Tuple t)
  {
    switch (t.getType()) {
      case SIMPLE_DATA:
      case PARTITIONED_DATA:
        LOG.debug("writing out " + t.getObject());
        SerDe serde = context.getSerDe();
        byte[] serialized = serde.toByteArray(t.getObject());
        try {
          output.write(serialized);
        }
        catch (IOException ex) {
          LOG.info("", ex);
        }
        break;

      default:
        LOG.debug("ignoring tuple " + t);
        break;
    }
  }

  /**
   * 
   * @return {@link com.malhartech.dag.StreamContext}
   */
  @Override
  public StreamContext getContext()
  {
    return this.context;
  }

  /**
   * 
   * @return boolean
   */
  public boolean hasFinished()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * 
   */
  @Override
  public void activate()
  {
  }
}
