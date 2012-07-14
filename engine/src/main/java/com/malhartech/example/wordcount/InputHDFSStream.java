/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.example.wordcount;

import com.malhartech.dag.StreamConfiguration;
import com.malhartech.stream.AbstractInputHDFSStream;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InputHDFSStream extends AbstractInputHDFSStream
{

  private static final Logger logger = LoggerFactory.getLogger(InputHDFSStream.class);
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
      logger.debug("getObject called returning " + wh.word);
      return wh;
    }

    logger.debug("getObject called returning null");

    return null;
  }
}
