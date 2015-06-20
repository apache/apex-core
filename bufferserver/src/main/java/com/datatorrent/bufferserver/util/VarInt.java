package com.datatorrent.bufferserver.util;

public class VarInt extends com.datatorrent.netlet.util.VarInt
{
  public static void read(SerializedData current)
  {
    final byte[] data = current.buffer;
    int offset = current.offset;

    byte tmp = data[offset++];
    if (tmp >= 0) {
      current.dataOffset = offset;
      current.length = tmp + offset - current.offset;
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
                current.length = result + offset - current.offset;
                return;
              }
            }

            current.length = -1;
            return;
          }
        }
      }
    }

    current.dataOffset = offset;
    current.length = result + offset - current.offset;
  }

}
