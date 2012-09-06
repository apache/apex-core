/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class MutatedSinkException extends RuntimeException
{
  private final Sink old;
  private final Sink sink;

  public MutatedSinkException(Sink old, Sink newSink, String message)
  {
    super(message);
    this.old = old;
    sink = newSink;
  }

  public final Sink getNewSink()
  {
    return sink;
  }

  public final Sink getOldSink()
  {
    return old;
  }
}
