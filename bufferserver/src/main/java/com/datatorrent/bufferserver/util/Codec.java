/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.util;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class Codec
{
  public static String getStringWindowId(long windowId)
  {
    return String.valueOf(windowId >> 32) + "[" + (int)windowId + "]";
  }

}
