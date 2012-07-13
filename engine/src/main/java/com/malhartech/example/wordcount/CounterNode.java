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

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class CounterNode extends AbstractNode
{

  private HashMap<String, Integer> words = new HashMap<String, Integer>();
  private boolean windowed; // if true the node should clear tuples at window boundary

  public CounterNode(NodeContext ctx)
  {
    super(ctx);
  }

  @Override
  public void setup(NodeConfiguration config)
  {
    config.getBoolean("windowed", true);
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
  public void process(NodeContext context, StreamContext streamContext, Object payload)
  {
    WordHolder wh = (WordHolder) payload;
    Integer i = words.get(wh.word);
    if (i == null) {
      words.put(wh.word, new Integer(wh.count));
    }
    else {
      i++;
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
  public void endWidndow(NodeContext context)
  {
    if (windowed) {
      for (Entry<String, Integer> entry : words.entrySet()) {
        WordHolder wh = new WordHolder();
        wh.word = entry.getKey();
        wh.count = entry.getValue();
        emit(wh);
      }
    }
  }
}
