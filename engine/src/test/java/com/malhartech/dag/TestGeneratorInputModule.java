/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.*;
import com.malhartech.api.Context.OperatorContext;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGeneratorInputModule implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(TestGeneratorInputModule.class);
  public static final String OUTPUT_PORT = "outputPort";
  public static final String KEY_MAX_TUPLES = "maxTuples";
  private String myConfigProperty;
  private int maxTuples;
  private int generatedTuples;
  private boolean autoGenerate = true;
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<Object> outport = new DefaultOutputPort<Object>(this);
  private transient volatile Thread autoGenerateThread;

  public int getMaxTuples()
  {
    return maxTuples;
  }

  public void setMaxTuples(int maxNumbers)
  {
    logger.info("setting max tuples to {}", maxNumbers);
    this.maxTuples = maxNumbers;
  }

  public void setAutogenerate(boolean flag)
  {
    autoGenerate = flag;
  }

  public String getMyConfigProperty()
  {
    return myConfigProperty;
  }

  public void setMyConfigProperty(String myConfigProperty)
  {
    this.myConfigProperty = myConfigProperty;
  }

  @Override
  public void replayEmitTuples(long windowId)
  {
  }

  /**
   * Manually add a tuple to emit.
   */
  public void emitTuple(String s)
  {
    outport.emit(s);
  }

  @Override
  public void postEmitTuples(long windowId, OutputPort<?> outputPort, Iterator<?> tuples)
  {
    if (maxTuples > 0) {
      while (tuples.hasNext()) {
        logger.debug("sent {}", tuples.next());
        if (++generatedTuples == maxTuples) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public void beginWindow()
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorConfiguration config)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void postActivate(OperatorContext ctx)
  {
    if (autoGenerate) {
      autoGenerateThread = new Thread()
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run()
        {
          try {
            if (maxTuples > 0) {
              for (int i = 1; i <= maxTuples; i++) {
                outport.emit(i);
                Thread.sleep(20);
              }
            }
            else {
              int i = 1;
              while (true) {
                outport.emit(i++);
                Thread.sleep(20);
              }
            }
          }
          catch (InterruptedException ex) {
            logger.info("Stopping generation of input because was interrupted");
          }
        }
      };
      autoGenerateThread.start();
    }
  }

  @Override
  public void preDeactivate()
  {
    autoGenerateThread = null;
  }
}
