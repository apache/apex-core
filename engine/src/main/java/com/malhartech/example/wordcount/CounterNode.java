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
  private boolean flagUp;

  public CounterNode(NodeContext ctx)
  {
    super(ctx);
  }

  @Override
  public void setup(NodeConfiguration config)
  {
    windowed = config.getBoolean("windowed", true);
  }

  @Override
  public void teardown()
  {
    if (!windowed) {
      for (Entry<String, Integer> entry : words.entrySet()) {
        emit(entry.getKey() + "\t" + entry.getValue() + "\n");
      }
    }
  }

  @Override
  public boolean shouldShutdown()
  {
    return shutdown;
  }

  @Override
  public void process(NodeContext context, StreamContext streamContext, Object payload)
  {
    WordHolder wh = (WordHolder) payload;
    if (wh.count == 0) {
      logger.info("finalword received, so would shutdown on completion of this window");
      flagUp = true;
    }

    Integer i = words.get(wh.word);
    if (i == null) {
      words.put(wh.word, new Integer(wh.count));
    }
    else {
      words.put(wh.word, i + wh.count);
    }
  }

  @Override
  public void beginWindow(NodeContext context)
  {
    if (windowed) {
      words.clear();
    }
  }

  @Override
  public void endWindow(NodeContext context)
  {
    if (windowed) {
      for (Entry<String, Integer> entry : words.entrySet()) {
        WordHolder wh = new WordHolder();
        wh.word = entry.getKey();
        wh.count = entry.getValue();
        emit(wh);
      }
    }

    if (flagUp) {
      shutdown = true;
    }
  }
}
