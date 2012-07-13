/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.example.wordcount;

import com.malhartech.dag.StreamConfiguration;
import com.malhartech.stream.AbstractInputHDFSStream;
import java.util.Scanner;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InputHDFSStream extends AbstractInputHDFSStream
{

  Scanner scanner = null;

  @Override
  public void setup(StreamConfiguration config)
  {
    super.setup(config);
    scanner = new Scanner(input);
  }

  @Override
  public Object getObject(Object object)
  {
    if (scanner.hasNext()) {
      WordHolder wh = new WordHolder();
      wh.word = scanner.next();
      wh.count = 1;
      return wh;
    }

    return null;
  }
}
