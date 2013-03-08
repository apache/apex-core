/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.util.Codec;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class Client extends malhar.netlet.Client
{
  protected byte[] readBuffer = new byte[32 * 1024];
  protected ByteBuffer buffer = ByteBuffer.wrap(readBuffer);
  protected int size, writeOffset, readOffset;

  @Override
  public ByteBuffer buffer()
  {
    return buffer;
  }

  // -ve number is no var int
  public int readVarInt()
  {
    if (readOffset < writeOffset) {
      int offset = readOffset;

      byte tmp = readBuffer[readOffset++];
      if (tmp >= 0) {
        return tmp;
      }
      else if (readOffset < writeOffset) {
        int integer = tmp & 0x7f;
        tmp = readBuffer[readOffset++];
        if (tmp >= 0) {
          return integer | tmp << 7;
        }
        else if (readOffset < writeOffset) {
          integer |= (tmp & 0x7f) << 7;
          tmp = readBuffer[readOffset++];

          if (tmp >= 0) {
            return integer | tmp << 14;
          }
          else if (readOffset < writeOffset) {
            integer |= (tmp & 0x7f) << 14;
            tmp = readBuffer[readOffset++];
            if (tmp >= 0) {
              return integer | tmp << 21;
            }
            else if (readOffset < writeOffset) {
              integer |= (tmp & 0x7f) << 21;
              tmp = readBuffer[readOffset++];
              if (tmp >= 0) {
                return integer | tmp << 28;
              }
              else {
                throw new NumberFormatException("Invalid varint at location " + offset + " => "
                        + Arrays.toString(Arrays.copyOfRange(readBuffer, offset, readOffset)));
              }
            }
          }
        }
      }

      readOffset = offset;
    }
    return -1;
  }

  @Override
  public void read(int len)
  {
    writeOffset += len;
    while (size == 0) {
      size = readVarInt();
      if (size == -1) {
        if (writeOffset == readBuffer.length) {
          if (readOffset > writeOffset - 5) {
            logger.info("hit the boundary while reading varint!");
            /*
             * we may be reading partial varint, adjust the buffers so that we have enough space to read the full data.
             */
            System.arraycopy(readBuffer, readOffset, readBuffer, 0, writeOffset - readOffset);
            writeOffset -= readOffset;
            buffer.position(writeOffset);
            readOffset = 0;
          }
        }
        else {
          return;
        }
      }
    }

    if (size > 0) {
      if (writeOffset - readOffset >= size) {
        onMessage(readBuffer, readOffset, size);
        readOffset += size;
      }
      else if (writeOffset == readBuffer.length) {
        if (size > readBuffer.length) {
          int newsize = readBuffer.length;
          while (newsize < size) {
            newsize <<= 1;
          }
          logger.info("resizing buffer to size {} from size {}", newsize, readBuffer.length);
          byte[] newArray = new byte[newsize];
          System.arraycopy(readBuffer, readOffset, newArray, 0, writeOffset - readOffset);
          writeOffset -= readOffset;
          readOffset = 0;
          buffer = ByteBuffer.wrap(newArray);
          buffer.position(writeOffset);
        }
        else {
          System.arraycopy(readBuffer, readOffset, readBuffer, 0, writeOffset - readOffset);
          writeOffset -= readOffset;
          readOffset = 0;
          buffer.clear();
          buffer.position(writeOffset);
        }
      }
      /* else need to read more */
    }
  }

  public void write(byte[] message)
  {
    write(message, 0, message.length);
  }

  private int intOffset;
  private static final int INT_ARRAY_SIZE = 4096 - 5;
  private byte[] intBuffer = new byte[INT_ARRAY_SIZE + 5];

  public void write(byte[] message, int offset, int size)
  {
    if (intOffset > INT_ARRAY_SIZE) {
      intBuffer = new byte[INT_ARRAY_SIZE + 5];
      intOffset = 0;
    }

    int newOffset = Codec.writeRawVarint32(size, intBuffer, intOffset);
    try {
      send(intBuffer, intOffset, newOffset - intOffset);
      intOffset = newOffset;
      send(message, offset, size);
    }
    catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  public abstract void onMessage(byte[] buffer, int offset, int size);

  private static final Logger logger = LoggerFactory.getLogger(Client.class);
}
