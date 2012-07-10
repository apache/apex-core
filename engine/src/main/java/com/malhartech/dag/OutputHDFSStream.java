/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class OutputHDFSStream
        implements Stream, Sink
{

  private StreamContext context;
  private FSDataOutputStream output;

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
      Logger.getLogger(AbstractInputHDFSStream.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

  @Override
  public void setContext(StreamContext context)
  {
    this.context = context;
  }

  @Override
  public void teardown()
  {
    try {
      output.close();
      output = null;
    }
    catch (IOException ex) {
      Logger.getLogger(OutputHDFSStream.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void doSomething(Tuple t)
  {
    switch (t.getData().getType()) {
      case SIMPLE_DATA:
      case PARTITIONED_DATA:
        SerDe serde = context.getSerDe();
        byte[] serialized = serde.toByteArray(t.getObject());
        try {
          output.write(serialized);
        }
        catch (IOException ex) {
          Logger.getLogger(OutputHDFSStream.class.getName()).log(Level.SEVERE, null, ex);
        }
        break;
    }
  }
}
