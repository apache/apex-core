/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.util;

import com.datatorrent.netlet.util.Slice;

/**
 * Wrapper for a {@code byte[]}, which provides read-only access and can "reveal" a partial slice of the underlying array.<p>
 *
 *
 * <b>Note:</b> Multibyte accessors all use big-endian order.
 *
 * @since 0.3.2
 */
public final class SerializedData extends Slice
{
  /**
   * the offset at which the actual data begins. Between offset and dataOffset, the length of the data is stored.
   */
  public int dataOffset;

  public SerializedData(byte[] array, int offset, int size)
  {
    super(array, offset, size);
  }

  private static final long serialVersionUID = 201596191703L;
}
