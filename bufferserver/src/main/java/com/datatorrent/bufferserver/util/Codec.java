/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.util;

/**
 * <p>Codec class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.9.1
 */
public class Codec
{
  public static String getStringWindowId(long windowId)
  {
    return String.valueOf(windowId >> 32) + '[' + (int)windowId + ']';
  }

  public static long getLongWindowId(String windowId)
  {
    int index = windowId.indexOf('[');
    return (Long.parseLong(windowId.substring(0, index)) << 32) | Integer.parseInt(windowId.substring(index + 1, windowId.length() - 1));
  }
}
