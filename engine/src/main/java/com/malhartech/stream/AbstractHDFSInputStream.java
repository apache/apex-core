/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.StreamConfiguration;
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
public abstract class AbstractHDFSInputStream extends AbstractInputAdapter implements Runnable
{

  private static final Logger logger = LoggerFactory.getLogger(AbstractHDFSInputStream.class);
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
      logger.error(ex.getLocalizedMessage());
    }

  }

  @Override
  public void activate()
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
    
    endStream();
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
