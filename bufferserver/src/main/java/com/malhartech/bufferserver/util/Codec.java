/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.util;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Codec
{
  /**
   * Writes the Variable Sized Integer of Length 32. Assumes that the buffer has 5 positions at least starting with offset.
   *
   * @param value
   * @param offset
   * @return int
   */
  public static int writeRawVarint32(int value, byte[] buffer, int offset)
  {
    while (true) {
      if ((value & ~0x7F) == 0) {
        buffer[offset++] = (byte) value;
        return offset;
      }
      else {
        buffer[offset++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }

  /**
   * 
   * @param current 
   */
  public static void readRawVarInt32(SerializedData current)
  {
    final byte[] data = current.bytes;
    int offset = current.offset;

    byte tmp = data[offset++];
    if (tmp >= 0) {
      current.dataOffset = offset;
      current.size = tmp + offset - current.offset;
      return;
    }
    int result = tmp & 0x7f;
    if ((tmp = data[offset++]) >= 0) {
      result |= tmp << 7;
    }
    else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = data[offset++]) >= 0) {
        result |= tmp << 14;
      }
      else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = data[offset++]) >= 0) {
          result |= tmp << 21;
        }
        else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = data[offset++]) << 28;
          if (tmp < 0) {
            // Discard upper 32 bits.
            for (int i = 0; i < 5; i++) {
              if (data[offset++] >= 0) {
                current.dataOffset = offset;
                current.size = result + offset - current.offset;
                return;
              }
            }
            
            current.size = -1;
            return;
          }
        }
      }
    }

    current.dataOffset = offset;
    current.size = result + offset - current.offset;
  }
}
