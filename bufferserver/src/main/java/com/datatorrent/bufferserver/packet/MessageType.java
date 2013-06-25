/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public enum MessageType
{
  NO_MESSAGE(0), PAYLOAD(1), RESET_WINDOW(2), BEGIN_WINDOW(3), END_WINDOW(4), END_STREAM(5), PUBLISHER_REQUEST(6), SUBSCRIBER_REQUEST(7), PURGE_REQUEST(8), RESET_REQUEST(9), CHECKPOINT(10), CODEC_STATE(11), NO_MESSAGE_ODD(127);
  public static final byte NO_MESSAGE_VALUE = 0;
  public static final byte PAYLOAD_VALUE = 1;
  public static final byte RESET_WINDOW_VALUE = 2;
  public static final byte BEGIN_WINDOW_VALUE = 3;
  public static final byte END_WINDOW_VALUE = 4;
  public static final byte END_STREAM_VALUE = 5;
  public static final byte PUBLISHER_REQUEST_VALUE = 6;
  public static final byte SUBSCRIBER_REQUEST_VALUE = 7;
  public static final byte PURGE_REQUEST_VALUE = 8;
  public static final byte RESET_REQUEST_VALUE = 9;
  public static final byte CHECKPOINT_VALUE = 10;
  public static final byte CODEC_STATE_VALUE = 11;
  public static final byte NO_MESSAGE_ODD_VALUE = 127;

  public final int getNumber()
  {
    return value;
  }

  public static MessageType valueOf(byte value)
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
      case 127:
        return NO_MESSAGE_ODD;
      default:
        return null;
    }
  }

  private final int value;

  private MessageType(int value)
  {
    this.value = value;
  }

}
