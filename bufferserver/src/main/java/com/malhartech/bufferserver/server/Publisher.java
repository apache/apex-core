/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.server;

import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.client.ProtoBufClient;
import com.malhartech.bufferserver.internal.DataList;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When the publisher connects to the server and starts publishing the data,
 * this is the end on the server side which handles all the communication.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
class Publisher extends ProtoBufClient
{
  private final DataList datalist;
  boolean dirty;

  Publisher(DataList dl)
  {
    this.datalist = dl;

    readBuffer = datalist.getBufer();
    buffer = ByteBuffer.wrap(readBuffer);
    buffer.position(datalist.getPosition());
  }

  public void transferBuffer(byte[] array, int offset, int len)
  {
    System.arraycopy(array, offset, readBuffer, writeOffset, len);
    buffer.position(len);
    read(len);
  }

  @Override
  public void onMessage(Message msg)
  {
    dirty = true;
  }

  @Override
  public void read(int len)
  {
    logger.debug("read {} bytes", len);
    writeOffset += len;
    do {
      if (size <= 0) {
        switch (size = readVarInt()) {
          case -1:
            if (writeOffset == readBuffer.length) {
              if (readOffset > writeOffset - 5) {
                /*
                 * if the data is not corrupt, we are limited by space to receive full varint.
                 * so we allocate a new buffer and copy over the partially written data to the
                 * new buffer and start as if we always had full room but not enough data.
                 */
                logger.info("hit the boundary while reading varint!");
                switchToNewBuffer(readBuffer, readOffset);
              }
            }

            if (dirty) {
              dirty = false;
              datalist.flush();
            }
            return;

          case 0:
            continue;
        }
      }

      if (writeOffset - readOffset >= size) {
        onMessage(readBuffer, readOffset, size);
        readOffset += size;
        size = 0;
      }
      else if (writeOffset == readBuffer.length) {
        /*
         * hit wall while writing serialized data, so have to allocate a new buffer.
         */
        switchToNewBuffer(null, 0);
      }
      else {
        if (dirty) {
          dirty = false;
          datalist.flush();
        }
        return;
      }
    }
    while (true);
  }

  public void switchToNewBuffer(byte[] array, int offset)
  {
    byte[] newBuffer = new byte[datalist.getBlockSize()];
    buffer = ByteBuffer.wrap(newBuffer);
    if (array == null || array.length - offset == 0) {
      writeOffset = 0;
    }
    else {
      writeOffset = array.length - offset;
      System.arraycopy(readBuffer, offset, newBuffer, 0, writeOffset);
      buffer.position(writeOffset);
    }
    readBuffer = newBuffer;
    readOffset = 0;
    datalist.addBuffer(readBuffer);
  }

  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
}
