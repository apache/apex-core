/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractInputHDFSStream extends AbstractInputObjectStream implements Runnable
{
  private static final long serialVersionUID = 201207061218L;

  private FSDataInputStream input;

  public void setup(StreamConfiguration config)
  {
    try {
      FileSystem fs = FileSystem.get(config);
      Path filepath = new Path(config.get("filepath"));
      input = fs.open(filepath);
    }
    catch (IOException ex) {
      Logger.getLogger(AbstractInputHDFSStream.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

  @Override
  public void setContext(StreamContext context)
  {
    super.setContext(context);
    Thread t = new Thread(this);
    t.start();
  }

  public void run()
  {
    Object o;
    while ((o = getObject(input)) != null) {
      context.getSink().doSomething(getTuple(o));
    }
  }

  public void teardown(StreamConfiguration config)
  {
    try {
      input.close();
    }
    catch (IOException ex) {
      Logger.getLogger(AbstractInputHDFSStream.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
