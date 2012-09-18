/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class FailedOperationException extends RuntimeException
{
  public FailedOperationException(String message)
  {
    super(message);
  }

  public FailedOperationException(Throwable cause)
  {
    super(cause);
  }
}
