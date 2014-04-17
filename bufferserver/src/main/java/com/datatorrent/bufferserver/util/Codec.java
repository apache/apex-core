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
  public static final long INVALID_WINDOW_ID = -1;
  public static final String UNPARSEABLE_WINDOW_ID = Long.toHexString(INVALID_WINDOW_ID);
  public static String getStringWindowId(long windowId)
  {
    if (windowId == INVALID_WINDOW_ID) {
      return UNPARSEABLE_WINDOW_ID;
    }

    return Long.toHexString(windowId);
  }

  public static long getLongWindowId(String windowId)
  {
    if (UNPARSEABLE_WINDOW_ID.equals(windowId)) {
      return INVALID_WINDOW_ID;
    }

    return Long.parseLong(windowId, 16);
  }
}
