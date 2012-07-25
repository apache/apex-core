/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.StreamConfiguration;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractHDFSInputStream extends AbstractObjectInputStream implements Runnable
{

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractHDFSInputStream.class);
  protected FSDataInputStream input;

  @Override
  public void setup(StreamConfiguration config)
  {
    try {
      FileSystem fs = FileSystem.get(config);
      Path filepath = new Path(config.get("filepath"));
      input = fs.open(filepath);
    }
    catch (IOException ex) {
      Logger.getLogger(AbstractHDFSInputStream.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

  public void activate()
  {
    Thread t = new Thread(this);
    t.start();    
  }
  
  public void run()
  {
    logger.debug("ready to read hdfs file");
    Object o;
    while ((o = getObject(input)) != null) {
      sendTuple(o);
    }
  }

  @Override
  public void teardown()
  {
    try {
      input.close();
    }
    catch (IOException ex) {
      Logger.getLogger(AbstractHDFSInputStream.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
