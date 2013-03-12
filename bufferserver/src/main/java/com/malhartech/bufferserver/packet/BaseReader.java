/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import java.util.Arrays;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class BaseReader
{
  private final byte[] buffer;
  private final int offset;
  private final int length;

  BaseReader(byte[] buffer, int offset, int length)
  {
    this.buffer = buffer;
    this.offset = offset;
    this.length = length;
  }

  public MessageType getType()
  {
    return MessageType.valueOf(buffer[offset]);
  }

  public int readVarInt(int offset, int limit)
  {
    if (offset < limit) {
      byte tmp = buffer[offset++];
      if (tmp >= 0) {
        return tmp;
      }
      else if (offset < limit) {
        int integer = tmp & 0x7f;
        tmp = buffer[offset++];
        if (tmp >= 0) {
          return integer | tmp << 7;
        }
        else if (offset < limit) {
          integer |= (tmp & 0x7f) << 7;
          tmp = buffer[offset++];

          if (tmp >= 0) {
            return integer | tmp << 14;
          }
          else if (offset < limit) {
            integer |= (tmp & 0x7f) << 14;
            tmp = buffer[offset++];
            if (tmp >= 0) {
              return integer | tmp << 21;
            }
            else if (offset < limit) {
              integer |= (tmp & 0x7f) << 21;
              tmp = buffer[offset++];
              if (tmp >= 0) {
                return integer | tmp << 28;
              }
              else {
                throw new NumberFormatException("Invalid varint at location " + offset + " => "
                        + Arrays.toString(Arrays.copyOfRange(buffer, offset, limit)));
              }
            }
          }
        }
      }
    }
    return -1;
  }

  public enum MessageType
  {
    NO_MESSAGE(0, 0),
    PAYLOAD(1, 1),
    RESET_WINDOW(2, 2),
    BEGIN_WINDOW(3, 3),
    END_WINDOW(4, 4),
    END_STREAM(5, 5),
    PUBLISHER_REQUEST(6, 6),
    SUBSCRIBER_REQUEST(7, 7),
    PURGE_REQUEST(8, 8),
    RESET_REQUEST(9, 9),
    CHECKPOINT(10, 10),
    CODEC_STATE(11, 11),
    NO_MESSAGE_ODD(12, 128),;
    public static final int NO_MESSAGE_VALUE = 0;
    public static final int PAYLOAD_VALUE = 1;
    public static final int RESET_WINDOW_VALUE = 2;
    public static final int BEGIN_WINDOW_VALUE = 3;
    public static final int END_WINDOW_VALUE = 4;
    public static final int END_STREAM_VALUE = 5;
    public static final int PUBLISHER_REQUEST_VALUE = 6;
    public static final int SUBSCRIBER_REQUEST_VALUE = 7;
    public static final int PURGE_REQUEST_VALUE = 8;
    public static final int RESET_REQUEST_VALUE = 9;
    public static final int CHECKPOINT_VALUE = 10;
    public static final int CODEC_STATE_VALUE = 11;
    public static final int NO_MESSAGE_ODD_VALUE = 128;

    public final int getNumber()
    {
      return value;
    }

    public static MessageType valueOf(int value)
    {
      switch (value) {
        case 0:
          return NO_MESSAGE;
        case 1:
          return PAYLOAD;
        case 2:
          return RESET_WINDOW;
        case 3:
          return BEGIN_WINDOW;
        case 4:
          return END_WINDOW;
        case 5:
          return END_STREAM;
        case 6:
          return PUBLISHER_REQUEST;
        case 7:
          return SUBSCRIBER_REQUEST;
        case 8:
          return PURGE_REQUEST;
        case 9:
          return RESET_REQUEST;
        case 10:
          return CHECKPOINT;
        case 11:
          return CODEC_STATE;
        case 128:
          return NO_MESSAGE_ODD;
        default:
          return null;
      }
    }

    private static final MessageType[] VALUES = {
      NO_MESSAGE, PAYLOAD, RESET_WINDOW, BEGIN_WINDOW, END_WINDOW, END_STREAM, PUBLISHER_REQUEST, SUBSCRIBER_REQUEST, PURGE_REQUEST, RESET_REQUEST, CHECKPOINT, CODEC_STATE, NO_MESSAGE_ODD,};
    private final int index;
    private final int value;

    private MessageType(int index, int value)
    {
      this.index = index;
      this.value = value;
    }

  }

}
