/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.util;

/**
 * Wrapper for a {@code byte[]}, which provides read-only access and can "reveal" a partial slice of the underlying array.<p>
 *
 *
 * <b>Note:</b> Multibyte accessors all use big-endian order.
 *
 * @since 0.3.2
 */
public final class SerializedData
{
  /**
   * the byte buffer where various messages including this one are stored.
   */
  public byte[] bytes;
  /**
   * the offset at which the current messages's length followed by the actual message is stored.
   */
  public int offset;
  /**
   * the offset at which the actual data begins. Between offset and dataOffset, the length of the data is stored.
   */
  public int dataOffset;
  /**
   * size is the total size of the slice of the byte array which stores the length and the message.
   */
  public int size;

  public SerializedData(byte[] array, int offset, int size)
  {
    bytes = array;
    this.offset = offset;
    this.size = size;
  }

  public boolean isEquivalent(SerializedData sd)
  {
    if (sd != null) {
      if (sd.size == size) {
        for (int i = size; i-- > 0;) {
          if (sd.bytes[sd.offset + i] != bytes[offset + i]) {
            return false;
          }
        }

        return true;
      }
    }

    return false;
  }

  /**
   *
   * @return String
   */
  @Override
  public String toString()
  {
    return "bytes = " + bytes + " offset = " + offset + " size = " + size;
  }

}
