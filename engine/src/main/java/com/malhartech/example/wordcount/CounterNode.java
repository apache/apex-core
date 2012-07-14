/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.example.wordcount;

import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.StreamContext;
import java.util.HashMap;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class CounterNode extends AbstractNode
{
  private static final Logger logger = LoggerFactory.getLogger(CounterNode.class);
  
  private HashMap<String, Integer> words = new HashMap<String, Integer>();
  private boolean windowed; // if true the node should clear tuples at window boundary
  private boolean shutdown;

  public CounterNode(NodeContext ctx)
  {
    super(ctx);
  }

  @Override
  public void setup(NodeConfiguration config)
  {
    windowed = config.getBoolean("windowed", true);
    logger.debug("setup called here TTTT " + windowed);
  }

  @Override
  public void teardown()
  {
    logger.debug("teardown called!");
    if (!windowed) {
      logger.debug("sinking " + words.size() + " words");
              
      for (Entry<String, Integer> entry : words.entrySet()) {
        emit(entry.getKey() + "\t" + entry.getValue() + "\n");
      }
    }
  }

  @Override
  public boolean shouldShutdown()
  {
    logger.debug("returning shutdown = " + shutdown);
    return shutdown;
  }
  
  @Override
  public void process(NodeContext context, StreamContext streamContext, Object payload)
  {
    WordHolder wh = (WordHolder) payload;
    if (wh == null) {
      logger.debug("process called with null data");
      shutdown = true;
    }
    else {
      logger.debug("process called with word " + wh.word + " and count = " + wh.count);
      Integer i = words.get(wh.word);
      if (i == null) {
        words.put(wh.word, new Integer(wh.count));
      }
      else {
        i++;
      }
    }
  }

  @Override
  public void beginWindow(NodeContext context)
  {
    logger.debug("begin down called when window = " + windowed);
    if (windowed) {
      words.clear();
    }
  }

  @Override
  public void endWidndow(NodeContext context)
  {
    logger.debug("endwindow called with " + words.size() + " entries and windowed = " + windowed);
    if (windowed) {
      for (Entry<String, Integer> entry : words.entrySet()) {
        WordHolder wh = new WordHolder();
        wh.word = entry.getKey();
        wh.count = entry.getValue();
        logger.debug("emitting " + wh.word + " with count = " + wh.count);
        emit(wh);
      }
    }
  }
}
